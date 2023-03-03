package worker

// test001ImportRecords imports 4 records; 4 < 5 (uploadCountThreshold) - upload is not triggered.
func (ts *testSuite) test001ImportRecords() {
	for i := 1; i <= 4; i++ {
		ts.Import(i)
	}
	ts.AssertEtcdState("001-import-records")
}
