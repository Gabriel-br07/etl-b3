from __future__ import annotations

from unittest.mock import patch

from app.etl.orchestration.prefect import serve as serve_module


def test_main_registers_daily_registry_and_intraday_deployments():
    fake_daily = object()
    fake_intra = object()
    with patch.object(
        serve_module.daily_registry_flow,
        "to_deployment",
        return_value=fake_daily,
    ) as daily_mock, patch.object(
        serve_module.intraday_quotes_flow,
        "to_deployment",
        return_value=fake_intra,
    ) as intra_mock, patch.object(serve_module, "serve") as serve_mock:
        serve_module.main()

    daily_mock.assert_called_once()
    assert daily_mock.call_args.kwargs["name"] == "daily-registry"
    assert daily_mock.call_args.kwargs["schedule"].timezone == "America/Sao_Paulo"

    intra_mock.assert_called_once()
    assert intra_mock.call_args.kwargs["name"] == "intraday-quotes"

    serve_mock.assert_called_once_with(fake_daily, fake_intra)


def test_main_skips_intraday_when_disabled(monkeypatch):
    monkeypatch.setenv("PREFECT_SERVE_INTRADAY", "false")
    fake_daily = object()
    with patch.object(
        serve_module.daily_registry_flow,
        "to_deployment",
        return_value=fake_daily,
    ), patch.object(
        serve_module.intraday_quotes_flow,
        "to_deployment",
    ) as intra_mock, patch.object(serve_module, "serve") as serve_mock:
        serve_module.main()

    intra_mock.assert_not_called()
    serve_mock.assert_called_once_with(fake_daily)
