package main

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

	"feature-flag-service/internal/constants"
)

//go:embed web/index.html
var webFS embed.FS

type FleetResponse struct {
	Namespace string       `json:"namespace"`
	Etcd      EtcdCluster  `json:"etcd"`
	HPA       []HPAStatus  `json:"hpa"`
	Pods      []PodDetail  `json:"pods"`
	Timestamp string       `json:"timestamp"`
}

type EtcdCluster struct {
	Evaluators  []NodeInfo `json:"evaluators"`
	ConnServers []NodeInfo `json:"conn_servers"`
}

type NodeInfo struct {
	NodeID string `json:"node_id,omitempty"`
	Slot   string `json:"slot,omitempty"`
	Addr   string `json:"addr"`
}

type HPAStatus struct {
	Name      string `json:"name"`
	Replicas  string `json:"replicas"`
	CPUActual string `json:"cpu_actual"`
	CPUTarget string `json:"cpu_target"`
}

type PodDetail struct {
	Name     string `json:"name"`
	Status   string `json:"status"`
	Ready    string `json:"ready"`
	Restarts string `json:"restarts"`
	CPU      string `json:"cpu"`
	Memory   string `json:"memory"`
	CPUNum   int    `json:"cpu_num"`
	MemNum   int    `json:"mem_num"`
}

type ChaosRequest struct {
	PodPrefix string `json:"pod_prefix"`
}

type ChaosResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

type VerificationResponse struct {
	AllPassed bool   `json:"all_passed"`
	Output    string `json:"output"`
	Error     string `json:"error,omitempty"`
}

var (
	k8sNamespace = "feature-flags"
	gatewayURL   = constants.DefaultK8sGatewayURL
	mgmtURL      = constants.DefaultK8sMgmtAPIURL
)

func main() {
	port := flag.String("port", constants.DefaultFleetWatchPort, "Port to serve Fleet Watch UI")
	ns := flag.String("namespace", constants.DefaultK8sNamespace, "Kubernetes namespace to monitor")
	gw := flag.String("gateway", constants.DefaultK8sGatewayURL, "Load balancer gateway URL for E2E verification")
	mgmt := flag.String("mgmt", constants.DefaultK8sMgmtAPIURL, "Management API URL for E2E verification")
	flag.Parse()

	k8sNamespace = *ns
	gatewayURL = *gw
	mgmtURL = *mgmt

	mux := http.NewServeMux()

	// 1. Static UI Web Assets
	subFS, err := fs.Sub(webFS, "web")
	if err != nil {
		log.Fatalf("Failed to create sub filesystem: %v", err)
	}
	mux.Handle("/", http.FileServer(http.FS(subFS)))

	// 2. REST API Handlers
	mux.HandleFunc("/api/fleet", handleFleet)
	mux.HandleFunc("/api/chaos/kill-pod", handleChaosKill)
	mux.HandleFunc("/api/verification/run", handleVerificationRun)

	addr := ":" + *port
	log.Printf("==================================================================")
	log.Printf("🚀 FLEET MISSION CONTROL UI STARTED")
	log.Printf("   URL:       http://localhost%s", addr)
	log.Printf("   Namespace: %s", k8sNamespace)
	log.Printf("==================================================================")

	server := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 60 * time.Second,
	}

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server error: %v", err)
	}
}

func handleFleet(w http.ResponseWriter, r *http.Request) {
	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)

	etcdData := getEtcdCluster(k8sNamespace)
	hpaData := getHPAStatuses(k8sNamespace)
	podData := getPodDetails(k8sNamespace)

	resp := FleetResponse{
		Namespace: k8sNamespace,
		Etcd:      etcdData,
		HPA:       hpaData,
		Pods:      podData,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}

	json.NewEncoder(w).Encode(resp)
}

func handleChaosKill(w http.ResponseWriter, r *http.Request) {
	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	var req ChaosRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.PodPrefix == "" {
		http.Error(w, `{"error":"pod_prefix required"}`, http.StatusBadRequest)
		return
	}

	// Find the targeted pod in the namespace matching the prefix
	cmd := exec.Command("kubectl", "get", "pods", "-n", k8sNamespace, "--no-headers", "-o", "custom-columns=:metadata.name")
	out, err := cmd.Output()
	if err != nil {
		json.NewEncoder(w).Encode(ChaosResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to list pods: %v", err),
		})
		return
	}

	var targetPod string
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	for _, line := range lines {
		pName := strings.TrimSpace(line)
		if strings.HasPrefix(pName, req.PodPrefix) || strings.Contains(pName, req.PodPrefix) {
			targetPod = pName
			break
		}
	}

	if targetPod == "" {
		json.NewEncoder(w).Encode(ChaosResponse{
			Success: false,
			Message: fmt.Sprintf("No active pod matched prefix '%s'", req.PodPrefix),
		})
		return
	}

	// Terminate the pod to simulate node crash / chaos failover
	delCmd := exec.Command("kubectl", "delete", "pod", targetPod, "-n", k8sNamespace, "--now")
	delOut, delErr := delCmd.CombinedOutput()
	if delErr != nil {
		json.NewEncoder(w).Encode(ChaosResponse{
			Success: false,
			Message: fmt.Sprintf("Delete failed: %s (%v)", string(delOut), delErr),
		})
		return
	}

	json.NewEncoder(w).Encode(ChaosResponse{
		Success: true,
		Message: fmt.Sprintf("Terminated pod '%s' (K8s will reschedule, etcd lease will failover)", targetPod),
	})
}

func handleVerificationRun(w http.ResponseWriter, r *http.Request) {
	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "./bin/verify-e2e",
		"-gateway="+gatewayURL,
		"-mgmt="+mgmtURL,
	)
	out, err := cmd.CombinedOutput()
	outputStr := string(out)

	passed := err == nil && strings.Contains(outputStr, "ALL END-TO-END INVARIANTS VERIFIED")

	resp := VerificationResponse{
		AllPassed: passed,
		Output:    outputStr,
	}
	if err != nil {
		resp.Error = err.Error()
	}

	json.NewEncoder(w).Encode(resp)
}

func getEtcdCluster(namespace string) EtcdCluster {
	var cluster EtcdCluster

	// Get etcd pod name
	podCmd := exec.Command("kubectl", "get", "pod", "-n", namespace, "-l", "app=etcd", "-o", "jsonpath={.items[0].metadata.name}")
	out, err := podCmd.Output()
	if err != nil || len(out) == 0 {
		return cluster
	}
	etcdPod := strings.TrimSpace(string(out))

	cmd := exec.Command("kubectl", "exec", "-n", namespace, "-c", "etcd", etcdPod, "--", "etcdctl", "get", "--prefix", "/members/")
	raw, err := cmd.Output()
	if err != nil {
		return cluster
	}

	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	for i := 0; i < len(lines)-1; i += 2 {
		k := strings.TrimSpace(lines[i])
		v := strings.TrimSpace(lines[i+1])
		if k == "" {
			continue
		}

		if strings.HasPrefix(k, constants.EtcdPrefixEvaluators) {
			nodeID := strings.TrimPrefix(k, constants.EtcdPrefixEvaluators)
			cluster.Evaluators = append(cluster.Evaluators, NodeInfo{
				NodeID: nodeID,
				Addr:   v,
			})
		} else if strings.HasPrefix(k, constants.EtcdPrefixConnServers) {
			slot := strings.TrimPrefix(k, constants.EtcdPrefixConnServers)
			cluster.ConnServers = append(cluster.ConnServers, NodeInfo{
				Slot: slot,
				Addr: v,
			})
		}
	}

	sort.Slice(cluster.Evaluators, func(i, j int) bool {
		return cluster.Evaluators[i].NodeID < cluster.Evaluators[j].NodeID
	})
	sort.Slice(cluster.ConnServers, func(i, j int) bool {
		return cluster.ConnServers[i].Slot < cluster.ConnServers[j].Slot
	})

	return cluster
}

type k8sHPAList struct {
	Items []struct {
		Metadata struct {
			Name string `json:"name"`
		} `json:"metadata"`
		Spec struct {
			MinReplicas *int `json:"minReplicas"`
			MaxReplicas int  `json:"maxReplicas"`
			Metrics     []struct {
				Resource struct {
					Target struct {
						AverageUtilization *int `json:"averageUtilization"`
					} `json:"target"`
				} `json:"resource"`
			} `json:"metrics"`
		} `json:"spec"`
		Status struct {
			CurrentReplicas int `json:"currentReplicas"`
			CurrentMetrics  []struct {
				Resource struct {
					Current struct {
						AverageUtilization *int `json:"averageUtilization"`
					} `json:"current"`
				} `json:"resource"`
			} `json:"currentMetrics"`
		} `json:"status"`
	} `json:"items"`
}

func getHPAStatuses(namespace string) []HPAStatus {
	cmd := exec.Command("kubectl", "get", "hpa", "-n", namespace, "-o", "json")
	out, err := cmd.Output()
	if err != nil {
		return nil
	}

	var list k8sHPAList
	if err := json.Unmarshal(out, &list); err != nil {
		return nil
	}

	var results []HPAStatus
	for _, item := range list.Items {
		cpuActual := "0%"
		if len(item.Status.CurrentMetrics) > 0 && item.Status.CurrentMetrics[0].Resource.Current.AverageUtilization != nil {
			cpuActual = fmt.Sprintf("%d%%", *item.Status.CurrentMetrics[0].Resource.Current.AverageUtilization)
		}
		cpuTarget := "50%"
		if len(item.Spec.Metrics) > 0 && item.Spec.Metrics[0].Resource.Target.AverageUtilization != nil {
			cpuTarget = fmt.Sprintf("%d%%", *item.Spec.Metrics[0].Resource.Target.AverageUtilization)
		}

		results = append(results, HPAStatus{
			Name:      item.Metadata.Name,
			Replicas:  strconv.Itoa(item.Status.CurrentReplicas),
			CPUActual: cpuActual,
			CPUTarget: cpuTarget,
		})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Name < results[j].Name
	})

	return results
}

func getPodDetails(namespace string) []PodDetail {
	cmd := exec.Command("kubectl", "get", "pods", "-n", namespace, "--no-headers")
	out, err := cmd.Output()
	if err != nil {
		return nil
	}

	metricsMap := getPodResourceMetrics(namespace)

	var list []PodDetail
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	for _, l := range lines {
		fields := strings.Fields(l)
		if len(fields) >= 4 {
			pName := fields[0]
			cpuStr := "-"
			memStr := "-"
			cpuNum := 0
			memNum := 0

			if m, ok := metricsMap[pName]; ok {
				cpuStr = m.CPU
				memStr = m.Memory
				cpuNum = parseMilliCPU(m.CPU)
				memNum = parseMiB(m.Memory)
			}

			list = append(list, PodDetail{
				Name:     pName,
				Ready:    fields[1],
				Status:   fields[2],
				Restarts: fields[3],
				CPU:      cpuStr,
				Memory:   memStr,
				CPUNum:   cpuNum,
				MemNum:   memNum,
			})
		}
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].Name < list[j].Name
	})

	return list
}

type podRawMetric struct {
	CPU    string
	Memory string
}

func getPodResourceMetrics(namespace string) map[string]podRawMetric {
	m := make(map[string]podRawMetric)
	cmd := exec.Command("kubectl", "top", "pods", "-n", namespace, "--no-headers")
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	if err := cmd.Run(); err != nil {
		return m
	}

	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	for _, l := range lines {
		f := strings.Fields(l)
		if len(f) >= 3 {
			m[f[0]] = podRawMetric{
				CPU:    f[1],
				Memory: f[2],
			}
		}
	}
	return m
}

func parseMilliCPU(s string) int {
	s = strings.TrimSpace(s)
	if strings.HasSuffix(s, "m") {
		v, _ := strconv.Atoi(strings.TrimSuffix(s, "m"))
		return v
	}
	v, err := strconv.Atoi(s)
	if err == nil {
		return v * 1000
	}
	return 0
}

func parseMiB(s string) int {
	s = strings.TrimSpace(s)
	if strings.HasSuffix(s, "Mi") {
		v, _ := strconv.Atoi(strings.TrimSuffix(s, "Mi"))
		return v
	}
	if strings.HasSuffix(s, "Gi") {
		v, _ := strconv.Atoi(strings.TrimSuffix(s, "Gi"))
		return v * 1024
	}
	if strings.HasSuffix(s, "Ki") {
		v, _ := strconv.Atoi(strings.TrimSuffix(s, "Ki"))
		return v / 1024
	}
	return 0
}
