import json
import sys
from unittest.mock import MagicMock, patch
import pytest

# ---------------------------------------------------------------------------
# 1. ORİJİNAL KODU DEĞİŞTİRMEDEN IMPORT ETME STRATEJİSİ
# prediction_service.py import edildiği an H2O, RabbitMQ ve Redis bağlantıları
# kurulmaya çalışıldığı ve channel.start_consuming() kilitlenme yaptığı için
# bu modülleri sys.modules seviyesinde önceden mock'luyoruz.
# ---------------------------------------------------------------------------

mock_h2o = MagicMock()
mock_rmq = MagicMock()
mock_redis = MagicMock()

# RabbitMQ Channel mock
mock_channel = MagicMock()
mock_rmq.rabbitmq_service.get_channel.return_value = mock_channel
# Top-level'da çalışan start_consuming()'in testi kilitlemesini engelliyoruz
mock_channel.start_consuming.return_value = None

# Model mock
mock_model = MagicMock()
mock_h2o.load_model.return_value = mock_model

# sys.modules enjeksiyonu ile orjinal 'prediction_service' dosyasını güvenle import ediyoruz
with patch.dict(
    sys.modules,
    {
        "h2o": mock_h2o,
        "Manager.rabbitmq_manager": mock_rmq,
        "Manager.Redis_Manager": mock_redis,
    },
):
    from Services import Prediction_Service 


# ---------------------------------------------------------------------------
# 2. TEST FIXTURE'LARI
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_ch():
    """RabbitMQ kanal parametresi mock'u."""
    return MagicMock()


@pytest.fixture
def mock_method():
    """RabbitMQ delivery_tag içeren method frame mock'u."""
    method = MagicMock()
    method.delivery_tag = 999
    return method


@pytest.fixture(autouse=True)
def reset_mocks():
    """Her test öncesi mock çağrı geçmişlerini temizler."""
    Prediction_Service.REDIS.reset_mock()
    Prediction_Service.model.reset_mock()
    yield


# ---------------------------------------------------------------------------
# 3. SENARYO BAZLI MODÜLER TESTLER
# ---------------------------------------------------------------------------


class TestPredictionServiceCallback:

    def test_callback_at_risk_true(self, mock_ch, mock_method):
        """Olasılık > 0.104433 olduğunda is_at_risk: True ve Redis kaydı doğrulama."""
        patient_data = {
            "patient_id": "P-101",
            "age": 65,
            "avg_glucose_level": 210.5,
        }
        body = json.dumps(patient_data).encode("utf-8")

        # predict["p1"][0, 0] erişimini simüle ediyoruz (Olasılık: %20 -> Risk Var)
        mock_predict_frame = MagicMock()
        mock_p1_col = MagicMock()
        mock_p1_col.__getitem__.return_value = 0.2000
        mock_predict_frame.__getitem__.return_value = mock_p1_col
        Prediction_Service.model.predict.return_value = mock_predict_frame

        # Callback fonksiyonunu çalıştır
        Prediction_Service.callback(mock_ch, mock_method, None, body)

        # Redis'e doğru veri yazıldı mı?
        Prediction_Service.REDIS.set_value.assert_called_once()
        args, kwargs = Prediction_Service.REDIS.set_value.call_args

        redis_key = args[0]
        redis_payload = json.loads(args[1])
        ttl = kwargs.get("ttl_sec")

        assert redis_key == "stroke_prediction:P-101"
        assert redis_payload["is_at_risk"] is True
        assert redis_payload["probability"] == 20.0
        assert redis_payload["status"] == "COMPLETED"
        assert ttl == 3600

        # basic_ack çağrıldı mı?
        mock_ch.basic_ack.assert_called_once_with(delivery_tag=999)
        mock_ch.basic_nack.assert_not_called()

    def test_callback_at_risk_false(self, mock_ch, mock_method):
        """Olasılık <= 0.104433 olduğunda is_at_risk: False doğrulama."""
        patient_data = {"patient_id": "P-102", "age": 25}
        body = json.dumps(patient_data).encode("utf-8")

        # Olasılık: %5 -> Risk Yok
        mock_predict_frame = MagicMock()
        mock_p1_col = MagicMock()
        mock_p1_col.__getitem__.return_value = 0.0500
        mock_predict_frame.__getitem__.return_value = mock_p1_col
        Prediction_Service.model.predict.return_value = mock_predict_frame

        Prediction_Service.callback(mock_ch, mock_method, None, body)

        # Redis verilerini kontrol et
        args, _ = Prediction_Service.REDIS.set_value.call_args
        redis_payload = json.loads(args[1])

        assert redis_payload["is_at_risk"] is False
        assert redis_payload["probability"] == 5.0
        mock_ch.basic_ack.assert_called_once_with(delivery_tag=999)

    def test_callback_json_decode_error(self, mock_ch, mock_method):
        """Bozuk JSON geldiğinde JSONDecodeError yakalama ve basic_nack doğrulama."""
        invalid_body = b"gecersiz_json_formati"

        Prediction_Service.callback(mock_ch, mock_method, None, invalid_body)

        # Hatalı mesajda nack atılmalı (requeue=False)
        mock_ch.basic_nack.assert_called_once_with(
            delivery_tag=999, requeue=False
        )
        mock_ch.basic_ack.assert_not_called()
        Prediction_Service.REDIS.set_value.assert_not_called()

    def test_callback_unexpected_exception(self, mock_ch, mock_method):
        """H2O model tahmini sırasında beklenmeyen hata oluştuğunda nack doğrulama."""
        patient_data = {"patient_id": "P-103"}
        body = json.dumps(patient_data).encode("utf-8")

        # Tahmin fonksiyonunun exception fırlatmasını simüle ediyoruz
        Prediction_Service.model.predict.side_effect = Exception(
            "H2O cluster unreachable"
        )

        Prediction_Service.callback(mock_ch, mock_method, None, body)

        mock_ch.basic_nack.assert_called_once_with(
            delivery_tag=999, requeue=False
        )
        mock_ch.basic_ack.assert_not_called()