//go:build !debugpprof

package main

// No-op stubs when built without -tags debugpprof

func pprofAddFlags() {}

func pprofInit(done <-chan struct{}, logOutput any) func() { return nil }
