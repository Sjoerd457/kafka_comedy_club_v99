import json
from kafka_comedy_club_v99.producer import FakeTTFData


def test_fake_ttf_data_generation():
    """Test the FakeTTFData class generates data correctly."""
    data = FakeTTFData()
    data_json = data.json()
    data_dict = json.loads(data_json)

    assert 'timestamp' in data_dict
    assert 'ticker' in data_dict
    assert 'open' in data_dict
    assert 'high' in data_dict
    assert 'low' in data_dict
    assert 'close' in data_dict
    assert data_dict['high'] >= data_dict['open'] >= data_dict['low'] <= data_dict['close']
