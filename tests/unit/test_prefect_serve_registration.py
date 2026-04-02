from __future__ import annotations

from unittest.mock import patch

from app.etl.orchestration.prefect import serve as serve_module


def test_main_registers_only_daily_deployment():
    fake_deployment = object()
    with patch.object(
        serve_module.daily_scraping_flow,
        "to_deployment",
        return_value=fake_deployment,
    ) as to_deployment_mock, patch.object(
        serve_module, "default_daily_parameters", return_value={"run_intraday": True}
    ), patch.object(serve_module, "serve") as serve_mock:
        serve_module.main()

    to_deployment_mock.assert_called_once()
    assert to_deployment_mock.call_args.kwargs["name"] == "daily-scraping"
    serve_mock.assert_called_once_with(fake_deployment)
    assert len(serve_mock.call_args.args) == 1
