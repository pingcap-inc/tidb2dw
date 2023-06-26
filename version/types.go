package version

import (
	"fmt"
	"runtime"
)

// TiDB2DWVersion is the semver of TiDB2DW
type TiDB2DWVersion struct {
	major int
	minor int
	patch int
	name  string
}

// NewTiDB2DWVersion creates a TiDB2DWVersion object
func NewTiDB2DWVersion() *TiDB2DWVersion {
	return &TiDB2DWVersion{
		major: TiDB2DWVerMajor,
		minor: TiDB2DWVerMinor,
		patch: TiDB2DWVerPatch,
		name:  TiDB2DWVerName,
	}
}

// Name returns the alternave name of TiDB2DWVersion
func (v *TiDB2DWVersion) Name() string {
	return v.name
}

// SemVer returns TiDB2DWVersion in semver format
func (v *TiDB2DWVersion) SemVer() string {
	return fmt.Sprintf("%d.%d.%d", v.major, v.minor, v.patch)
}

// String converts TiDB2DWVersion to a string
func (v *TiDB2DWVersion) String() string {
	return fmt.Sprintf("%s %s\n%s", v.SemVer(), v.name, NewTiDB2DWBuildInfo())
}

// TiDB2DWBuild is the info of building environment
type TiDB2DWBuild struct {
	GitHash   string `json:"gitHash"`
	GitRef    string `json:"gitRef"`
	GoVersion string `json:"goVersion"`
}

// NewTiDB2DWBuildInfo creates a TiDB2DWBuild object
func NewTiDB2DWBuildInfo() *TiDB2DWBuild {
	return &TiDB2DWBuild{
		GitHash:   GitHash,
		GitRef:    GitRef,
		GoVersion: runtime.Version(),
	}
}

// String converts TiDB2DWBuild to a string
func (v *TiDB2DWBuild) String() string {
	return fmt.Sprintf("Go Version: %s\nGit Ref: %s\nGitHash: %s", v.GoVersion, v.GitRef, v.GitHash)
}
