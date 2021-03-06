// Code generated by MockGen. DO NOT EDIT.
// Source: ./consumer/consumer.go

// Package mock is a generated GoMock package.
package mock

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
)

// MockReceivingManager is a mock of ReceivingManager interface.
type MockReceivingManager struct {
	ctrl     *gomock.Controller
	recorder *MockReceivingManagerMockRecorder
}

// MockReceivingManagerMockRecorder is the mock recorder for MockReceivingManager.
type MockReceivingManagerMockRecorder struct {
	mock *MockReceivingManager
}

// NewMockReceivingManager creates a new mock instance.
func NewMockReceivingManager(ctrl *gomock.Controller) *MockReceivingManager {
	mock := &MockReceivingManager{ctrl: ctrl}
	mock.recorder = &MockReceivingManagerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockReceivingManager) EXPECT() *MockReceivingManagerMockRecorder {
	return m.recorder
}

// OpenConnAndProcess mocks base method.
func (m *MockReceivingManager) OpenConnAndProcess() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "OpenConnAndProcess")
}

// OpenConnAndProcess indicates an expected call of OpenConnAndProcess.
func (mr *MockReceivingManagerMockRecorder) OpenConnAndProcess() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OpenConnAndProcess", reflect.TypeOf((*MockReceivingManager)(nil).OpenConnAndProcess))
}
