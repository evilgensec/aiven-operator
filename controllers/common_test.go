package controllers

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/aiven/aiven-operator/api/v1alpha1"
	kafkaconnectuserconfig "github.com/aiven/aiven-operator/api/v1alpha1/userconfig/integration/kafka_connect"
)

// TestCreateEmptyUserConfiguration shouldn't panic
func TestCreateEmptyUserConfiguration(t *testing.T) {
	var uc *kafkaconnectuserconfig.KafkaConnectUserConfig
	m, err := CreateUserConfiguration(uc)
	assert.Empty(t, m)
	assert.NoError(t, err)
}

func TestConnectionSecretName(t *testing.T) {
	tests := map[string]struct {
		targetName string
		want       string
	}{
		"uses resource name by default": {
			want: "pg",
		},
		"uses connInfoSecretTarget name when set": {
			targetName: "custom-secret",
			want:       "custom-secret",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			obj := &mockObjWithSecret{
				name: "pg",
				target: v1alpha1.ConnInfoSecretTarget{
					Name: tt.targetName,
				},
			}

			assert.Equal(t, tt.want, connectionSecretName(obj))
		})
	}
}

func TestPowerStateAnnotations(t *testing.T) {
	tests := []struct {
		annotation string
		wantOn     bool
		wantOff    bool
	}{
		{annotation: "false", wantOff: true},
		{annotation: "true", wantOn: true},
		{wantOn: false, wantOff: false},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("case %d", i), func(t *testing.T) {
			obj := &v1alpha1.PostgreSQL{}
			if tt.annotation != "" {
				obj.SetAnnotations(map[string]string{
					instanceIsRunningAnnotation: tt.annotation,
				})
			}

			require.Equal(t, tt.wantOn, IsMarkedAsPoweredOn(obj))
			require.Equal(t, tt.wantOff, IsMarkedAsPoweredOff(obj))
		})
	}
}

type mockObjWithSecret struct {
	name   string
	target v1alpha1.ConnInfoSecretTarget
}

func (t *mockObjWithSecret) GetName() string {
	return t.name
}

func (t *mockObjWithSecret) GetNamespace() string {
	return "default"
}

func (t *mockObjWithSecret) GetObjectKind() schema.ObjectKind {
	return schema.EmptyObjectKind
}

func (t *mockObjWithSecret) GetConnInfoSecretTarget() v1alpha1.ConnInfoSecretTarget {
	return t.target
}
