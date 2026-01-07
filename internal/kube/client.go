package kube

import (
	"fmt"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// NewClientset builds a clientset for a given kubeconfig context.
func NewClientset(context string) (*kubernetes.Clientset, error) {
	loader := clientcmd.NewDefaultClientConfigLoadingRules()
	overrides := &clientcmd.ConfigOverrides{CurrentContext: context}
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loader, overrides).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("failed building kubeconfig for context %s: %w", context, err)
	}
	return kubernetes.NewForConfig(cfg)
}

// RESTConfig returns the underlying config for advanced clients.
func RESTConfig(context string) (*rest.Config, error) {
	loader := clientcmd.NewDefaultClientConfigLoadingRules()
	overrides := &clientcmd.ConfigOverrides{CurrentContext: context}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loader, overrides).ClientConfig()
}
