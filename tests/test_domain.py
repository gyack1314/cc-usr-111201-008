"""核对领域原语的业务数值。"""
import unittest
from analysis_data.io import load
from analysis_data.domain import summarize
class DomainTests(unittest.TestCase):
    def test_raw_retransmissions(self):
        normal=summarize(load('data/normal.json')[0]);repeat=summarize(load('data/out-of-order.json')[0])
        self.assertEqual(normal,repeat);self.assertEqual(normal['site-a'],{'anomalies':3,'received':6})
        self.assertEqual(summarize(load('data/exact-threshold.json')[0])['one']['anomalies'],2)
