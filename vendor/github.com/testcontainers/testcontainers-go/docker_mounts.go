package testcontainers

import (
	"errors"
	"path/filepath"

	"github.com/docker/docker/api/types/mount"

	"github.com/testcontainers/testcontainers-go/log"
)

var mountTypeMapping = map[MountType]mount.Type{
	MountTypeBind:   mount.TypeBind, // Deprecated, it will be removed in a future release
	MountTypeVolume: mount.TypeVolume,
	MountTypeTmpfs:  mount.TypeTmpfs,
	MountTypePipe:   mount.TypeNamedPipe,
	MountTypeImage:  mount.TypeImage,
}

// Deprecated: use Files or HostConfigModifier in the ContainerRequest, or copy files container APIs to make containers portable across Docker environments
// BindMounter can optionally be implemented by mount sources
// to support advanced scenarios based on mount.BindOptions
type BindMounter interface {
	GetBindOptions() *mount.BindOptions
}

// VolumeMounter can optionally be implemented by mount sources
// to support advanced scenarios based on mount.VolumeOptions
type VolumeMounter interface {
	GetVolumeOptions() *mount.VolumeOptions
}

// TmpfsMounter can optionally be implemented by mount sources
// to support advanced scenarios based on mount.TmpfsOptions
type TmpfsMounter interface {
	GetTmpfsOptions() *mount.TmpfsOptions
}

// ImageMounter can optionally be implemented by mount sources
// to support advanced scenarios based on mount.ImageOptions
type ImageMounter interface {
	ImageOptions() *mount.ImageOptions
}

// Deprecated: use Files or HostConfigModifier in the ContainerRequest, or copy files container APIs to make containers portable across Docker environments
type DockerBindMountSource struct {
	*mount.BindOptions

	// HostPath is the path mounted into the container
	// the same host path might be mounted to multiple locations within a single container
	HostPath string
}

// Deprecated: use Files or HostConfigModifier in the ContainerRequest, or copy files container APIs to make containers portable across Docker environments
func (s DockerBindMountSource) Source() string {
	return s.HostPath
}

// Deprecated: use Files or HostConfigModifier in the ContainerRequest, or copy files container APIs to make containers portable across Docker environments
func (DockerBindMountSource) Type() MountType {
	return MountTypeBind
}

// Deprecated: use Files or HostConfigModifier in the ContainerRequest, or copy files container APIs to make containers portable across Docker environments
func (s DockerBindMountSource) GetBindOptions() *mount.BindOptions {
	return s.BindOptions
}

type DockerVolumeMountSource struct {
	*mount.VolumeOptions

	// Name refers to the name of the volume to be mounted
	// the same volume might be mounted to multiple locations within a single container
	Name string
}

func (s DockerVolumeMountSource) Source() string {
	return s.Name
}

func (DockerVolumeMountSource) Type() MountType {
	return MountTypeVolume
}

func (s DockerVolumeMountSource) GetVolumeOptions() *mount.VolumeOptions {
	return s.VolumeOptions
}

type DockerTmpfsMountSource struct {
	GenericTmpfsMountSource
	*mount.TmpfsOptions
}

func (s DockerTmpfsMountSource) GetTmpfsOptions() *mount.TmpfsOptions {
	return s.TmpfsOptions
}

// DockerImageMountSource is a mount source for an image
type DockerImageMountSource struct {
	// imageName is the image name
	imageName string

	// subpath is the subpath to mount the image into
	subpath string
}

// NewDockerImageMountSource creates a new DockerImageMountSource
func NewDockerImageMountSource(imageName string, subpath string) DockerImageMountSource {
	return DockerImageMountSource{
		imageName: imageName,
		subpath:   subpath,
	}
}

// Validate validates the source of the mount, ensuring that the subpath is a relative path
func (s DockerImageMountSource) Validate() error {
	if !filepath.IsLocal(s.subpath) {
		return errors.New("image mount source must be a local path")
	}
	return nil
}

// ImageOptions returns the image options for the image mount
func (s DockerImageMountSource) ImageOptions() *mount.ImageOptions {
	return &mount.ImageOptions{
		Subpath: s.subpath,
	}
}

// Source returns the image name for the image mount
func (s DockerImageMountSource) Source() string {
	return s.imageName
}

// Type returns the mount type for the image mount
func (s DockerImageMountSource) Type() MountType {
	return MountTypeImage
}

// PrepareMounts maps the given []ContainerMount to the corresponding
// []mount.Mount for further processing
func (m ContainerMounts) PrepareMounts() []mount.Mount {
	return mapToDockerMounts(m)
}

// mapToDockerMounts maps the given []ContainerMount to the corresponding
// []mount.Mount for further processing
func mapToDockerMounts(containerMounts ContainerMounts) []mount.Mount {
	mounts := make([]mount.Mount, 0, len(containerMounts))

	for idx := range containerMounts {
		m := containerMounts[idx]

		var mountType mount.Type
		if mt, ok := mountTypeMapping[m.Source.Type()]; ok {
			mountType = mt
		} else {
			continue
		}

		containerMount := mount.Mount{
			Type:     mountType,
			Source:   m.Source.Source(),
			ReadOnly: m.ReadOnly,
			Target:   m.Target.Target(),
		}

		switch typedMounter := m.Source.(type) {
		case VolumeMounter:
			containerMount.VolumeOptions = typedMounter.GetVolumeOptions()
		case TmpfsMounter:
			containerMount.TmpfsOptions = typedMounter.GetTmpfsOptions()
		case ImageMounter:
			containerMount.ImageOptions = typedMounter.ImageOptions()
		case BindMounter:
			log.Printf("Mount type %s is not supported by Testcontainers for Go", m.Source.Type())
		default:
			// The provided source type has no custom options
		}

		if mountType == mount.TypeVolume {
			if containerMount.VolumeOptions == nil {
				containerMount.VolumeOptions = &mount.VolumeOptions{
					Labels: make(map[string]string),
				}
			}
			AddGenericLabels(containerMount.VolumeOptions.Labels)
		}

		mounts = append(mounts, containerMount)
	}

	return mounts
}
