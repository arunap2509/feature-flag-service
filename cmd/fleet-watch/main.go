package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"
)

type PodMetric struct {
	Name   string
	CPU    string
	Memory string
}

type PodStatus struct {
	Name     string
	Ready    string
	Status   string
	Restarts string
	Age      string
	IP       string
}

type HPAInfo struct {
	Name       string
	Reference  string
	Targets    string
	MinPods    string
	MaxPods    string
	Replicas   string
}

func main() {
	nsFlag := flag.String("namespace", "feature-flags", "Kubernetes namespace to monitor")
	intervalFlag := flag.Duration("interval", 1*time.Second, "Dashboard refresh interval")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ticker := time.NewTicker(*intervalFlag)
	defer ticker.Stop()

	// Initial render
	renderDashboard(*nsFlag)

	for {
		select {
		case <-ctx.Done():
			fmt.Println("\n[fleet-watch] Exiting monitoring dashboard.")
			return
		case <-ticker.C:
			renderDashboard(*nsFlag)
		}
	}
}

func renderDashboard(namespace string) {
	// Gather etcd data
	etcdMembers := getEtcdMembers(namespace)

	// Gather HPA status
	hpaList := getHPAStatus(namespace)

	// Gather pod statuses
	pods := getPodStatuses(namespace)

	// Gather pod resource metrics (top pods)
	metrics := getPodMetrics(namespace)

	// Clear terminal screen and reset cursor to top
	fmt.Print("\033[H\033[2J")

	now := time.Now().Format("15:04:05 MST")
	fmt.Println("==========================================================================================")
	fmt.Printf("⚡ [fleet-watch] FEATURE FLAG CLUSTER REAL-TIME FLEET MONITOR | %s | Namespace: %s\n", now, namespace)
	fmt.Println("==========================================================================================")

	// Section 1: etcd Topology
	fmt.Println("🔷 1. ETCD DISTRIBUTED TOPOLOGY & CONSISTENT HASH RING SLOTS")
	evalNodes, connSlots := splitEtcdMembers(etcdMembers)
	if len(evalNodes) == 0 && len(connSlots) == 0 {
		fmt.Println("   (No active members discovered in etcd yet)")
	} else {
		fmt.Printf("   Evaluators on Hash Ring (%d nodes active):\n", len(evalNodes))
		for _, e := range evalNodes {
			fmt.Printf("     • %-32s -> %s\n", e.Key, e.Value)
		}
		fmt.Printf("   Edge Connection Server Slots (%d active upstreams):\n", len(connSlots))
		for _, c := range connSlots {
			fmt.Printf("     • %-32s -> %s\n", c.Key, c.Value)
		}
	}
	fmt.Println()

	// Section 2: Autoscaling (HPA) Status
	fmt.Println("📈 2. AUTOSCALING (HPA) DYNAMIC TARGETS & LIVE REPLICAS")
	if len(hpaList) == 0 {
		fmt.Println("   (No HorizontalPodAutoscalers active)")
	} else {
		fmt.Printf("   %-20s | %-12s | %-18s | %-12s\n", "NAME", "REPLICAS", "CPU USAGE / TARGET", "MIN / MAX")
		fmt.Printf("   %s\n", strings.Repeat("-", 68))
		for _, h := range hpaList {
			fmt.Printf("   %-20s | %-12s | %-18s | %s / %s\n", h.Name, h.Replicas, h.Targets, h.MinPods, h.MaxPods)
		}
	}
	fmt.Println()

	// Section 3: Live Pod Fleet & CPU/Memory
	fmt.Println("🖥️  3. POD FLEET & REAL-TIME RESOURCE UTILIZATION")
	fmt.Printf("   %-34s | %-8s | %-8s | %-8s | %-10s | %-10s\n", "POD NAME", "STATUS", "READY", "RESTARTS", "CPU", "MEMORY")
	fmt.Printf("   %s\n", strings.Repeat("-", 88))

	metricsMap := make(map[string]PodMetric)
	for _, m := range metrics {
		metricsMap[m.Name] = m
	}

	for _, p := range pods {
		cpu := "-"
		mem := "-"
		if m, ok := metricsMap[p.Name]; ok {
			cpu = m.CPU
			mem = m.Memory
		}
		statusColor := p.Status
		if p.Status == "Running" && p.Ready == "1/1" {
			statusColor = "Running"
		}
		fmt.Printf("   %-34s | %-8s | %-8s | %-8s | %-10s | %-10s\n",
			p.Name, statusColor, p.Ready, p.Restarts, cpu, mem)
	}

	fmt.Println("==========================================================================================")
	fmt.Println("💡 Tip: Run './bin/sim-operator' or './bin/sim-clients' in another terminal to watch auto-scaling.")
	fmt.Println("   Press Ctrl+C to stop monitoring.")
}

type EtcdEntry struct {
	Key   string
	Value string
}

func getEtcdMembers(namespace string) []EtcdEntry {
	// Discover etcd pod
	podCmd := exec.Command("kubectl", "get", "pod", "-n", namespace, "-l", "app=etcd", "-o", "jsonpath={.items[0].metadata.name}")
	out, err := podCmd.Output()
	if err != nil || len(out) == 0 {
		return nil
	}
	etcdPod := strings.TrimSpace(string(out))

	cmd := exec.Command("kubectl", "exec", "-n", namespace, "-c", "etcd", etcdPod, "--", "etcdctl", "get", "--prefix", "/members/")
	raw, err := cmd.Output()
	if err != nil {
		return nil
	}

	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	var entries []EtcdEntry
	for i := 0; i < len(lines)-1; i += 2 {
		k := strings.TrimSpace(lines[i])
		v := strings.TrimSpace(lines[i+1])
		if k != "" {
			entries = append(entries, EtcdEntry{Key: k, Value: v})
		}
	}
	return entries
}

func splitEtcdMembers(entries []EtcdEntry) (evals []EtcdEntry, conns []EtcdEntry) {
	for _, e := range entries {
		if strings.Contains(e.Key, "evaluators") {
			evals = append(evals, e)
		} else if strings.Contains(e.Key, "conn-servers") {
			conns = append(conns, e)
		}
	}
	sort.Slice(evals, func(i, j int) bool { return evals[i].Key < evals[j].Key })
	sort.Slice(conns, func(i, j int) bool { return conns[i].Key < conns[j].Key })
	return
}

func getHPAStatus(namespace string) []HPAInfo {
	cmd := exec.Command("kubectl", "get", "hpa", "-n", namespace, "--no-headers")
	out, err := cmd.Output()
	if err != nil {
		return nil
	}

	var results []HPAInfo
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	for _, l := range lines {
		f := strings.Fields(l)
		if len(f) >= 6 {
			results = append(results, HPAInfo{
				Name:      f[0],
				Reference: f[1],
				Targets:   f[2],
				MinPods:   f[3],
				MaxPods:   f[4],
				Replicas:  f[5],
			})
		}
	}
	return results
}

func getPodStatuses(namespace string) []PodStatus {
	cmd := exec.Command("kubectl", "get", "pods", "-n", namespace, "--no-headers")
	out, err := cmd.Output()
	if err != nil {
		return nil
	}

	var list []PodStatus
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	for _, l := range lines {
		fields := strings.Fields(l)
		if len(fields) >= 5 {
			list = append(list, PodStatus{
				Name:     fields[0],
				Ready:    fields[1],
				Status:   fields[2],
				Restarts: fields[3],
				Age:      fields[4],
			})
		}
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })
	return list
}

func getPodMetrics(namespace string) []PodMetric {
	cmd := exec.Command("kubectl", "top", "pods", "-n", namespace, "--no-headers")
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	if err := cmd.Run(); err != nil {
		return nil
	}

	var list []PodMetric
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	for _, l := range lines {
		fields := strings.Fields(l)
		if len(fields) >= 3 {
			list = append(list, PodMetric{
				Name:   fields[0],
				CPU:    fields[1],
				Memory: fields[2],
			})
		}
	}
	return list
}
