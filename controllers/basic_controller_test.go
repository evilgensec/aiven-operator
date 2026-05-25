package controllers

import (
	"context"
	"testing"

	avngen "github.com/aiven/go-client-codegen"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/aiven/aiven-operator/api/v1alpha1"
)

func TestInstanceReconcilerHelper_updateInstanceStateAndSecretUntilRunning(t *testing.T) {
	connectionSecret := func(pg *v1alpha1.PostgreSQL) *corev1.Secret {
		return &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pg.Name,
				Namespace: pg.Namespace,
			},
			StringData: map[string]string{
				"PGPASSWORD": "pw",
			},
		}
	}

	t.Run("Marks resource not ready when connection Secret publish fails", func(t *testing.T) {
		pg := newReadyPostgreSQL(t)
		controller := true

		scheme := newBasicControllerTestScheme(t)
		k8sClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(&corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pg.Name,
					Namespace: pg.Namespace,
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "aiven.io/v1alpha1",
							Kind:       "PostgreSQL",
							Name:       "other",
							UID:        types.UID("other-uid"),
							Controller: &controller,
						},
					},
				},
			}).
			Build()

		h := NewMockHandlers(t)
		h.EXPECT().
			get(mock.Anything, mock.Anything, pg).
			Run(func(_ context.Context, _ avngen.Client, _ client.Object) {
				metav1.SetMetaDataAnnotation(&pg.ObjectMeta, instanceIsRunningAnnotation, "true")
				meta.SetStatusCondition(&pg.Status.Conditions, getRunningCondition(metav1.ConditionTrue, "CheckRunning", "Instance is running on Aiven side"))
			}).
			Return(connectionSecret(pg), nil).
			Once()

		helper := &instanceReconcilerHelper{
			k8s: k8sClient,
			h:   h,
			rec: record.NewFakeRecorder(10),
		}

		err := helper.updateInstanceStateAndSecretUntilRunning(t.Context(), pg)
		require.Error(t, err)

		require.False(t, hasIsRunningAnnotation(pg))

		running := meta.FindStatusCondition(pg.Status.Conditions, conditionTypeRunning)
		require.NotNil(t, running)
		require.Equal(t, metav1.ConditionUnknown, running.Status)
		require.Equal(t, string(errConditionConnInfoSecret), running.Reason)

		errCond := meta.FindStatusCondition(pg.Status.Conditions, ConditionTypeError)
		require.NotNil(t, errCond)
		require.Equal(t, metav1.ConditionUnknown, errCond.Status)
		require.Equal(t, string(errConditionConnInfoSecret), errCond.Reason)
	})

	t.Run("Clears connection Secret publish error after successful publish", func(t *testing.T) {
		pg := newReadyPostgreSQL(t)
		meta.SetStatusCondition(&pg.Status.Conditions, getErrorCondition(errConditionConnInfoSecret, assert.AnError))

		scheme := newBasicControllerTestScheme(t)
		k8sClient := fake.NewClientBuilder().
			WithScheme(scheme).
			Build()

		h := NewMockHandlers(t)
		h.EXPECT().
			get(mock.Anything, mock.Anything, pg).
			Run(func(_ context.Context, _ avngen.Client, _ client.Object) {
				metav1.SetMetaDataAnnotation(&pg.ObjectMeta, instanceIsRunningAnnotation, "true")
				meta.SetStatusCondition(&pg.Status.Conditions, getRunningCondition(metav1.ConditionTrue, "CheckRunning", "Instance is running on Aiven side"))
			}).
			Return(connectionSecret(pg), nil).
			Once()

		helper := &instanceReconcilerHelper{
			k8s: k8sClient,
			h:   h,
			rec: record.NewFakeRecorder(10),
		}

		require.NoError(t, helper.updateInstanceStateAndSecretUntilRunning(t.Context(), pg))
		require.True(t, hasIsRunningAnnotation(pg))
		require.Nil(t, meta.FindStatusCondition(pg.Status.Conditions, ConditionTypeError))

		got := &corev1.Secret{}
		require.NoError(t, k8sClient.Get(t.Context(), types.NamespacedName{Name: pg.Name, Namespace: pg.Namespace}, got))
		require.Equal(t, []byte("pw"), got.Data["PGPASSWORD"])
	})
}

func TestInstanceReconcilerHelper_resourceNeedsConnectionSecretSync(t *testing.T) {
	ownedSecret := func(pg *v1alpha1.PostgreSQL) *corev1.Secret {
		controller := true
		return &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pg.Name,
				Namespace: pg.Namespace,
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "aiven.io/v1alpha1",
						Kind:       "PostgreSQL",
						Name:       pg.Name,
						UID:        pg.UID,
						Controller: &controller,
					},
				},
			},
			Data: map[string][]byte{
				"PGPASSWORD": []byte("pw"),
			},
		}
	}

	tests := map[string]struct {
		mutate        func(*v1alpha1.PostgreSQL)
		objects       []runtime.Object
		wantNeedsSync bool
	}{
		"does not need sync for powered-off resource": {
			mutate: func(pg *v1alpha1.PostgreSQL) {
				metav1.SetMetaDataAnnotation(&pg.ObjectMeta, instanceIsRunningAnnotation, "false")
			},
			wantNeedsSync: false,
		},
		"does not need sync when connection Secret creation is disabled": {
			mutate: func(pg *v1alpha1.PostgreSQL) {
				pg.Spec.ConnInfoSecretTargetDisabled = new(bool)
				*pg.Spec.ConnInfoSecretTargetDisabled = true
			},
			wantNeedsSync: false,
		},
		"needs sync when connection Secret is missing": {
			wantNeedsSync: true,
		},
		"needs sync when connection Secret is not controlled by the resource": {
			objects: []runtime.Object{
				&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "pg-no-ref", Namespace: "default"}},
			},
			wantNeedsSync: true,
		},
		"needs sync when a stale connection Secret error is present": {
			mutate: func(pg *v1alpha1.PostgreSQL) {
				meta.SetStatusCondition(&pg.Status.Conditions, getErrorCondition(errConditionConnInfoSecret, assert.AnError))
			},
			objects: []runtime.Object{
				ownedSecret(newReadyPostgreSQL(t)),
			},
			wantNeedsSync: true,
		},
		"does not need sync when connection Secret is controlled by the resource": {
			objects: []runtime.Object{
				ownedSecret(newReadyPostgreSQL(t)),
			},
			wantNeedsSync: false,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			pg := newReadyPostgreSQL(t)
			if tt.mutate != nil {
				tt.mutate(pg)
			}

			scheme := newBasicControllerTestScheme(t)
			k8sClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithRuntimeObjects(tt.objects...).
				Build()

			helper := &instanceReconcilerHelper{k8s: k8sClient}
			require.Equal(t, tt.wantNeedsSync, helper.resourceNeedsConnectionSecretSync(t.Context(), pg))
		})
	}
}

func newBasicControllerTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, v1alpha1.AddToScheme(scheme))
	return scheme
}

func newReadyPostgreSQL(t *testing.T) *v1alpha1.PostgreSQL {
	t.Helper()

	pg := newObjectFromYAML[v1alpha1.PostgreSQL](t, yamlPostgres)
	pg.UID = types.UID("pg-uid")
	pg.Generation = 1
	metav1.SetMetaDataAnnotation(&pg.ObjectMeta, processedGenerationAnnotation, "1")
	metav1.SetMetaDataAnnotation(&pg.ObjectMeta, instanceIsRunningAnnotation, "true")
	meta.SetStatusCondition(&pg.Status.Conditions, getRunningCondition(metav1.ConditionTrue, "CheckRunning", "Instance is running on Aiven side"))
	return pg
}
