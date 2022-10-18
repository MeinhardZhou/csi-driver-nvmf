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
	"fmt"
	"os"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"k8s.io/klog"
	"k8s.io/utils/exec"
	"k8s.io/utils/mount"
)

type nvmfDiskInfo struct {
	VolName    string
	Nqn        string
	DeviceUUID string
	Transport  *nvmfTransport
}

type nvmfTransport struct {
	trAddr string
	trPort string
	trType string
}

type nvmfDiskMounter struct {
	nvmfDiskInfo *nvmfDiskInfo
	readOnly     bool
	fsType       string
	mountOptions []string
	mounter      *mount.SafeFormatAndMount
	exec         exec.Interface
	targetPath   string
	connector    *nvmfConnector
}

type nvmfDiskUnMounter struct {
	mounter mount.Interface
	exec    exec.Interface
}

func getNVMfDiskInfo(req *csi.NodePublishVolumeRequest) (*nvmfDiskInfo, error) {
	volName := req.GetVolumeId()

	volOpts := req.GetVolumeContext()
	targetTrAddr := volOpts["targetTrAddr"]
	targetTrPort := volOpts["targetTrPort"]
	targetTrType := volOpts["targetTrType"]
	deviceUUID := volOpts["deviceUUID"]
	nqn := volOpts["nqn"]

	if targetTrAddr == "" || nqn == "" || targetTrPort == "" || targetTrType == "" {
		return nil, fmt.Errorf("Some Nvme target info is missing, volID: %s ", volName)
	}

	return &nvmfDiskInfo{
		VolName:    volName,
		Nqn:        nqn,
		DeviceUUID: deviceUUID,
		Transport: &nvmfTransport{
			trAddr: targetTrAddr,
			trPort: targetTrPort,
			trType: targetTrType,
		},
	}, nil
}

func getNVMfDiskMounter(nvmfInfo *nvmfDiskInfo, req *csi.NodePublishVolumeRequest) *nvmfDiskMounter {
	readOnly := req.GetReadonly()
	fsType := req.GetVolumeCapability().GetMount().GetFsType()
	mountOptions := req.GetVolumeCapability().GetMount().GetMountFlags()

	return &nvmfDiskMounter{
		nvmfDiskInfo: nvmfInfo,
		readOnly:     readOnly,
		fsType:       fsType,
		mountOptions: mountOptions,
		mounter:      &mount.SafeFormatAndMount{Interface: mount.New(""), Exec: exec.New()},
		exec:         exec.New(),
		targetPath:   req.GetTargetPath(),
		connector:    getNvmfConnector(nvmfInfo),
	}
}

func getNVMfDiskUnMounter(req *csi.NodeUnpublishVolumeRequest) *nvmfDiskUnMounter {
	return &nvmfDiskUnMounter{
		mounter: mount.New(""),
		exec:    exec.New(),
	}
}

func AttachDisk(req *csi.NodePublishVolumeRequest, nm nvmfDiskMounter) (string, error) {
	if nm.connector == nil {
		return "", fmt.Errorf("connector is nil")
	}

	// connect nvmf target disk
	devicePath, err := nm.connector.Connect()
	if err != nil {
		klog.Errorf("AttachDisk: VolumeID %s failed to connect, Error: %v", req.VolumeId, err)
		return "", err
	}
	if devicePath == "" {
		klog.Errorf("AttachDisk: VolumeID %s get nil devicePath", req.VolumeId)
		return "", fmt.Errorf("VolumeID %s get nil devicePath", req.VolumeId)
	}
	klog.Infof("AttachDisk: Volume %s successful connected, Device：%s", req.VolumeId, devicePath)

	mntPath := nm.targetPath
	klog.Infof("AttachDisk: MntPath: %s", mntPath)
	notMounted, err := nm.mounter.IsLikelyNotMountPoint(mntPath)
	if err != nil && !os.IsNotExist(err) {
		return "", fmt.Errorf("Heuristic determination of mount point failed: %v", err)
	}
	if !notMounted {
		klog.Infof("AttachDisk: TargetPath: %s is already mounted for VolumeID %s and DevicePath", nm.targetPath, req.VolumeId, devicePath)
		return "", nil
	}

	// prepare for mount
	if err := os.MkdirAll(mntPath, 0750); err != nil {
		klog.Errorf("AttachDisk: failed to mkdir %s. Error: %v", mntPath, err)
		return "", err
	}

	err = persistConnectorFile(nm.connector, mntPath+".json")
	if err != nil {
		klog.Errorf("AttachDisk: failed to persist connection info. Error: %v", err)
		return "", fmt.Errorf("unable to create persistence file for connection")
	}

	var options []string
	if nm.readOnly {
		options = append(options, "ro")
	} else {
		options = append(options, "rw")
	}
	options = append(options, nm.mountOptions...)
	err = nm.mounter.FormatAndMount(devicePath, mntPath, nm.fsType, options)
	if err != nil {
		klog.Errorf("AttachDisk: failed to mount Device %s to %s with options: %v. Error: %v", devicePath, mntPath, options, err)
		nm.connector.Disconnect()
		removeConnectorFile(mntPath)
		return "", fmt.Errorf("failed to mount Device %s to %s with options: %v. Error: %v", devicePath, mntPath, options, err)
	}

	klog.Infof("AttachDisk: Successfully Mount DevicePath %s to %s with options: %v", devicePath, mntPath, options)
	return devicePath, nil
}

func DetachDisk(volumeID string, num *nvmfDiskUnMounter, targetPath string) error {
	_, cnt, err := mount.GetDeviceNameFromMount(num.mounter, targetPath)
	if err != nil {
		klog.Errorf("DetachDisk: failed to get device from mnt: %s. Error: %v", targetPath, err)
		return err
	}
	if pathExists, pathErr := mount.PathExists(targetPath); pathErr != nil {
		return fmt.Errorf("Error checking if path exists. Error: %v", pathErr)
	} else if !pathExists {
		klog.Warningf("DetachDisk: unmount skipped because path does not exist: %v", targetPath)
		return nil
	}
	if err = num.mounter.Unmount(targetPath); err != nil {
		klog.Errorf("DetachDisk: failed to unmount targetPath %s. Error: %v", targetPath, err)
		return err
	}
	cnt--
	if cnt != 0 {
		return nil
	}

	connector, err := GetConnectorFromFile(targetPath + ".json")
	if err != nil {
		klog.Errorf("DetachDisk: failed to get connector from ConnectorPath %s.json. Error: %v", targetPath, err)
		return err
	}
	err = connector.Disconnect()
	if err != nil {
		klog.Errorf("DetachDisk: failed to disconnect targetPath %s of VolumeID %s. Error: %v", targetPath, volumeID, err)
		return err
	}
	removeConnectorFile(targetPath)
	return nil
}
