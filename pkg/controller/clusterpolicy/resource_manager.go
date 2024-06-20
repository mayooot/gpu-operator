package clusterpolicy

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	promv1 "github.com/coreos/prometheus-operator/pkg/apis/monitoring/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	schedv1 "k8s.io/api/scheduling/v1beta1"

	secv1 "github.com/openshift/api/security/v1"

	"k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/client-go/kubernetes/scheme"
)

type assetsFromFile []byte

var manifests []assetsFromFile

// Resources indicates resources managed by GPU operator
type Resources struct {
	ServiceAccount             corev1.ServiceAccount
	Role                       rbacv1.Role
	RoleBinding                rbacv1.RoleBinding
	ClusterRole                rbacv1.ClusterRole
	ClusterRoleBinding         rbacv1.ClusterRoleBinding
	ConfigMap                  corev1.ConfigMap
	DaemonSet                  appsv1.DaemonSet
	Deployment                 appsv1.Deployment
	Pod                        corev1.Pod
	Service                    corev1.Service
	ServiceMonitor             promv1.ServiceMonitor
	PriorityClass              schedv1.PriorityClass
	Taint                      corev1.Taint
	SecurityContextConstraints secv1.SecurityContextConstraints
}

func filePathWalkDir(root string) ([]string, error) {
	var files []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Info("DEBUG: error in filepath.Walk on %s: %v", root, err)
			return nil
		}
		if !info.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	return files, err
}

func getAssetsFrom(path, openshiftVersion string) []assetsFromFile {
	manifests := []assetsFromFile{}
	files, err := filePathWalkDir(path)
	if err != nil {
		panic(err)
	}
	sort.Strings(files)
	for _, file := range files {
		if strings.Contains(file, "openshift") && openshiftVersion == "" {
			continue
		}

		buffer, err := ioutil.ReadFile(file)
		if err != nil {
			panic(err)
		}
		manifests = append(manifests, buffer)
	}
	return manifests
}

// 获取给定的 path 目录下的 yaml 文件，通过 yaml 文件中的 Kind 字段确定要 K8s 资源类型
// 然后根据不同类型注册不同的处理函数
// 最后返回需要的资源类型 res，里面含有具体的资源配置信息
// 和每个资源的处理函数 ctrl
func addResourcesControls(path, openshiftVersion string) (Resources, controlFunc) {
	res := Resources{}
	ctrl := controlFunc{}

	log.Info("Getting assets from: ", "path:", path)
	// 从给定的 path 下读取文件
	manifests := getAssetsFrom(path, openshiftVersion)

	// 创建解析 yaml 的工具
	s := json.NewYAMLSerializer(json.DefaultMetaFactory, scheme.Scheme,
		scheme.Scheme)
	reg, _ := regexp.Compile(`\b(\w*kind:\w*)\B.*\b`)

	for _, m := range manifests {
		// 使用正则表达式寻找 kind 关键字
		kind := reg.FindString(string(m))
		slce := strings.Split(kind, ":")
		kind = strings.TrimSpace(slce[1])

		log.Info("DEBUG: Looking for ", "Kind", kind, "in path:", path)

		switch kind {
		case "Role":
			// 将 yaml 文件内容反序列化到 res.ServiceAccount 里
			_, _, err := s.Decode(m, nil, &res.Role)
			panicIfError(err)
			// 并将处理函数添加到 ctrl
			ctrl = append(ctrl, Role)
		case "ServiceAccount":
			_, _, err := s.Decode(m, nil, &res.ServiceAccount)
			panicIfError(err)
			ctrl = append(ctrl, ServiceAccount)
		case "RoleBinding":
			_, _, err := s.Decode(m, nil, &res.RoleBinding)
			panicIfError(err)
			ctrl = append(ctrl, RoleBinding)
		case "ClusterRole":
			_, _, err := s.Decode(m, nil, &res.ClusterRole)
			panicIfError(err)
			ctrl = append(ctrl, ClusterRole)
		case "ClusterRoleBinding":
			_, _, err := s.Decode(m, nil, &res.ClusterRoleBinding)
			panicIfError(err)
			ctrl = append(ctrl, ClusterRoleBinding)
		case "ConfigMap":
			_, _, err := s.Decode(m, nil, &res.ConfigMap)
			panicIfError(err)
			ctrl = append(ctrl, ConfigMap)
		case "DaemonSet":
			_, _, err := s.Decode(m, nil, &res.DaemonSet)
			panicIfError(err)
			// 重点看一下 DaemonSet 的处理函数
			ctrl = append(ctrl, DaemonSet)
		case "Deployment":
			_, _, err := s.Decode(m, nil, &res.Deployment)
			panicIfError(err)
			ctrl = append(ctrl, Deployment)
		case "Service":
			_, _, err := s.Decode(m, nil, &res.Service)
			panicIfError(err)
			ctrl = append(ctrl, Service)
		case "Pod":
			_, _, err := s.Decode(m, nil, &res.Pod)
			panicIfError(err)
			ctrl = append(ctrl, Pod)
		case "ServiceMonitor":
			_, _, err := s.Decode(m, nil, &res.ServiceMonitor)
			panicIfError(err)
			ctrl = append(ctrl, ServiceMonitor)
		case "SecurityContextConstraints":
			_, _, err := s.Decode(m, nil, &res.SecurityContextConstraints)
			panicIfError(err)
			ctrl = append(ctrl, SecurityContextConstraints)
		default:
			log.Info("Unknown Resource", "Manifest", m, "Kind", kind)
		}

	}

	return res, ctrl
}

func panicIfError(err error) {
	if err != nil {
		panic(err)
	}
}
