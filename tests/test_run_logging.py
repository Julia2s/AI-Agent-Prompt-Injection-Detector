import pytest

from mlops.run_logging import quiet_run


@pytest.mark.parametrize("fails", [False, True])
def test_quiet_run_preserves_status_and_suppresses_exit_output(fails, capsys):
    class Run:
        status = None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            self.status = "FAILED" if exc_type else "FINISHED"
            print("MLflow run URL")

    run = Run()
    if fails:
        with pytest.raises(ValueError, match="failure"), quiet_run(run):
            raise ValueError("failure")
    else:
        with quiet_run(run) as active:
            assert active is run
    assert run.status == ("FAILED" if fails else "FINISHED")
    assert capsys.readouterr().out == ""
