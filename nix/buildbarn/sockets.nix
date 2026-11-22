# The Unix sockets between roles on one machine, under `runtimeDirectory`.
run: {
  storage = "${run}/storage.sock";
  frontend = "${run}/frontend.sock";
  schedulerClient = "${run}/scheduler-client.sock";
  schedulerWorker = "${run}/scheduler-worker.sock";
  runner = "${run}/runner.sock";
}
