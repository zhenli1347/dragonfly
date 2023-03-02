package controller

import (
	"context"
	"fmt"
	"time"

	resourcesv1 "dragonflydb.io/dragonfly/api/v1alpha1"
	"dragonflydb.io/dragonfly/internal/resources"
	"github.com/go-redis/redis"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func waitForStatefulSetReady(ctx context.Context, c client.Client, name, namespace string, maxDuration time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, maxDuration)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for statefulset to be ready")
		default:
			// Check if the statefulset is ready
			ready, err := isStatefulSetReady(ctx, c, name, namespace)
			if err != nil {
				return err
			}
			if ready {
				return nil
			}
		}
	}
}

func isStatefulSetReady(ctx context.Context, c client.Client, name, namespace string) (bool, error) {
	var statefulSet appsv1.StatefulSet
	if err := c.Get(ctx, types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}, &statefulSet); err != nil {
		return false, err
	}

	if statefulSet.Status.ReadyReplicas == statefulSet.Status.Replicas {
		return true, nil
	}

	return false, nil
}

func findHealthyAndMarkActive(ctx context.Context, c client.Client, db *resourcesv1.DragonflyDb) error {
	log := log.FromContext(ctx)
	log.Info(fmt.Sprintf("Finding healthy and marking active for %s", db.Name))

	pods := corev1.PodList{}
	if err := c.List(ctx, &pods, client.InNamespace(db.Namespace), client.MatchingLabels{
		"app":                              db.Name,
		resources.KubernetesPartOfLabelKey: "dragonflydb",
	}); err != nil {
		log.Error(err, "could not list Pods")
		return err
	}

	var master string
	var masterIp string
	for _, pod := range pods.Items {
		if pod.Status.Phase == corev1.PodRunning && pod.Status.ContainerStatuses[0].Ready {
			master = pod.Name
			masterIp = pod.Status.PodIP
			if err := markActive(ctx, c, db, pod.Name); err != nil {
				return err
			}
			break
		}
	}

	// Mark others as replicas
	for _, pod := range pods.Items {
		if pod.Name != master {
			if err := markReplica(ctx, c, db, pod.Status.PodIP, masterIp); err != nil {
				return err
			}
		}
	}

	return nil
}

func markReplica(ctx context.Context, client client.Client, db *resourcesv1.DragonflyDb, podIp string, masterIp string) error {
	log := log.FromContext(ctx)
	log.Info(fmt.Sprintf("Marking %s as replica", podIp))

	redisClient := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:6379", podIp),
	})

	resp, err := redisClient.SlaveOf(masterIp, "6379").Result()
	if err != nil {
		return err
	}

	if resp != "OK" {
		return fmt.Errorf("could not mark instance as active")
	}

	return nil
}

func markActive(ctx context.Context, client client.Client, db *resourcesv1.DragonflyDb, podName string) error {
	log := log.FromContext(ctx)
	log.Info(fmt.Sprintf("Marking %s as active", podName))

	pod := corev1.Pod{}
	if err := client.Get(ctx, types.NamespacedName{
		Namespace: db.Namespace,
		Name:      podName,
	}, &pod); err != nil {
		log.Error(err, "could not get Pod")
		return err
	}

	pod.Labels["active"] = "true"

	if err := client.Update(ctx, &pod); err != nil {
		log.Error(err, "could not update Pod")
		return err
	}

	if err := markDragonflyInstanceAsActive(ctx, pod.Status.PodIP); err != nil {
		log.Error(err, "could not mark instance as active")
		return err
	}

	return nil
}

// Marks this instance as active in the cluster
func markDragonflyInstanceAsActive(ctx context.Context, podIp string) error {
	redisClient := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:6379", podIp),
	})

	resp, err := redisClient.SlaveOf("NO", "ONE").Result()
	if err != nil {
		return err
	}

	if resp != "OK" {
		return fmt.Errorf("could not mark instance as active")
	}

	return nil
}
