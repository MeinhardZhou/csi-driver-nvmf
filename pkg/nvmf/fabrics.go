/*
Copyright 2021 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package nvmf

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/kubernetes-csi/csi-driver-nvmf/pkg/utils"
	"k8s.io/klog"
)

type nvmfConnector struct {
	VolumeID      string
	DeviceUUID    string
	TargetNqn     string
	TargetAddr    string
	TargetPort    string
	TransportType string
	RetryCount    int32
	CheckInterval int32
}

func getNvmfConnector(nvmfInfo *nvmfDiskInfo) *nvmfConnector {
	return &nvmfConnector{
		VolumeID:      nvmfInfo.VolName,
		DeviceUUID:    nvmfInfo.DeviceUUID,
		TargetNqn:     nvmfInfo.Nqn,
		TargetAddr:    nvmfInfo.Transport.trAddr,
		TargetPort:    nvmfInfo.Transport.trPort,
		TransportType: nvmfInfo.Transport.trType,
	}
}

// connector provides a struct to hold all of the needed parameters to make nvmf connection

func _connect(nvmf_connect_args string) error {
	file, err := os.OpenFile("/dev/nvme-fabrics", os.O_RDWR, 0666)
	defer file.Close()
	if err != nil {
		klog.Errorf("Connect: failed to open NVMf fabrics file. Error: %v", err)
		return err
	}

	err = utils.WriteStringToFile(file, nvmf_connect_args)
	if err != nil {
		klog.Errorf("Connect: failed to write args to NVMf fabrics file. Error: %v", err)
		return err
	}
	// todo: read file to verify
	lines, err := utils.ReadLinesFromFile(file)
	klog.Infof("Connect: read string %s", lines)
	return nil
}

func _disconnect(sysfs_path string) error {
	file, err := os.OpenFile(sysfs_path, os.O_WRONLY, 0755)
	if err != nil {
		return err
	}
	err = utils.WriteStringToFile(file, "1")
	if err != nil {
		klog.Errorf("Disconnect: failed to write to delete_controller. Error: %v", err)
		return err
	}
	return nil
}

func disconnectSubsys(nqn, ctrl string) (res bool) {
	sysfs_nqn_path := fmt.Sprintf("%s/%s/subsysnqn", SYS_NVMF, ctrl)
	sysfs_del_path := fmt.Sprintf("%s/%s/delete_controller", SYS_NVMF, ctrl)

	file, err := os.Open(sysfs_nqn_path)
	defer file.Close()
	if err != nil {
		klog.Errorf("Disconnect: failed to open file %s. Error: %v", file.Name(), err)
		return false
	}

	lines, err := utils.ReadLinesFromFile(file)
	if err != nil {
		klog.Errorf("Disconnect: failed to read file %s. Error: %v", file.Name(), err)
		return false
	}

	if lines[0] != nqn {
		klog.Warningf("Disconnect: not this subsystem, skip")
		return false
	}

	err = _disconnect(sysfs_del_path)
	if err != nil {
		klog.Errorf("Disconnect: failed to disconnect nvmf. Error: %s", err)
		return false
	}

	return true
}

func disconnectByNqn(nqn string) int {
	ret := 0
	if len(nqn) > NVMF_NQN_SIZE {
		klog.Errorf("Disconnect: nqn %s is too long ", nqn)
		return -EINVAL
	}

	devices, err := ioutil.ReadDir(SYS_NVMF)
	if err != nil {
		klog.Errorf("Disconnect: failed to readdir %s. Error: %s", SYS_NVMF, err)
		return -ENOENT
	}

	for _, device := range devices {
		if disconnectSubsys(nqn, device.Name()) {
			ret++
		}
	}
	return ret
}

// connect to volume to this node and return devicePath
func (c *nvmfConnector) Connect() (string, error) {
	if c.RetryCount == 0 {
		c.RetryCount = 10
	}
	if c.CheckInterval == 0 {
		c.CheckInterval = 1
	}

	if c.RetryCount < 0 || c.CheckInterval < 0 {
		return "", fmt.Errorf("Invalid RetryCount and CheckInterval combinaitons. RetryCount: %d, CheckInterval: %d ", c.RetryCount, c.CheckInterval)
	}

	if strings.ToLower(c.TransportType) != "tcp" && strings.ToLower(c.TransportType) != "rdma" {
		return "", fmt.Errorf("nvmf transport only support tcp/rdma ")
	}

	nvmfConnectArgs := fmt.Sprintf("nqn=%s,transport=%s,traddr=%s,trsvcid=%s", c.TargetNqn, c.TransportType, c.TargetAddr, c.TargetPort)

	// connect nvmf disk to Node
	err := _connect(nvmfConnectArgs)
	if err != nil {
		return "", err
	}
	klog.Infof("Connect Volume %s success nqn: %s", c.VolumeID, c.TargetNqn)

	devicePath := strings.Join([]string{SYS_DEV_NVMF_BASE_PATH, c.DeviceUUID}, ".")
	retries := int(c.RetryCount / c.CheckInterval)
	if exists, err := waitFordevicePathToExist(devicePath, retries, int(c.CheckInterval), c.TransportType); !exists {
		klog.Errorf("failed to connect nqn %s. Error %v, rollback to disconnect nvmf disk", c.TargetNqn, err)
		ret := disconnectByNqn(c.TargetNqn)
		if ret < 0 {
			klog.Errorf("rollback error !!!")
		}
		return "", err
	}

	klog.Infof("Connect nvmf disk to devicePath: %s", devicePath)
	return devicePath, nil
}

// we disconnect only by nqn
func (c *nvmfConnector) Disconnect() error {
	ret := disconnectByNqn(c.TargetNqn)
	if ret == 0 {
		return fmt.Errorf("Disconnect: failed to disconnect by nqn. Error: %s ", c.TargetNqn)
	}

	return nil
}

// PersistConnector persists the provided Connector to the specified file (ie /var/lib/pfile/myConnector.json)
func persistConnectorFile(c *nvmfConnector, filePath string) error {
	f, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create nvmf persistence file %s. Error: %s", filePath, err)
	}
	defer f.Close()
	encoder := json.NewEncoder(f)
	if err = encoder.Encode(c); err != nil {
		return fmt.Errorf("error encoding connector: %v", err)
	}
	return nil

}

func removeConnectorFile(targetPath string) {
	// todo: here maybe be attack for os.Remove can operate any file, fix?
	if err := os.Remove(targetPath + ".json"); err != nil {
		klog.Errorf("DetachDisk: Can't remove connector file: %s", targetPath)
	}
	if err := os.RemoveAll(targetPath); err != nil {
		klog.Errorf("DetachDisk: failed to remove mount path Error: %v", err)
	}
}

func GetConnectorFromFile(filePath string) (*nvmfConnector, error) {
	f, err := ioutil.ReadFile(filePath)
	if err != nil {
		return &nvmfConnector{}, err

	}
	data := nvmfConnector{}
	err = json.Unmarshal([]byte(f), &data)
	if err != nil {
		return &nvmfConnector{}, err
	}

	return &data, nil
}
