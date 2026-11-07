import ast
from pathlib import Path


def test_training_uses_shared_logging_without_print_calls():
    path = Path(__file__).resolve().parents[1] / "mlops" / "train.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]
    assert not any(isinstance(call.func, ast.Name) and call.func.id == "print" for call in calls)
    assert any(
        isinstance(call.func, ast.Name)
        and call.func.id == "setup_logging"
        and len(call.args) == 1
        and isinstance(call.args[0], ast.Constant)
        and call.args[0].value == "train"
        for call in calls
    )
