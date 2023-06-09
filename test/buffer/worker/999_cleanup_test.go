package worker

// test999Cleanup tests the deletion of all objects.
func (ts *testSuite) test999Cleanup() {
	ts.t.Logf("-------------------------")
	ts.t.Logf("999 cleanup")
	ts.t.Logf("-------------------------")

	// Delete export1
	ts.DeleteExport(ts.export1.ReceiverID, ts.export1.ID)

	// Delete receiver (and export2)
	ts.DeleteReceiver(ts.receiver.ID)

	// Check etcd state
	ts.AssertEtcdState("999-cleanup")

	ts.AssertNoLoggedWarning()
	ts.AssertNoLoggedError()
	ts.TruncateLogs()
}
