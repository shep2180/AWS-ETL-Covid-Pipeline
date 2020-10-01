# necessary libraries
import unittest

from module_etl import extract_nyt, extract_jh

class TestETLFunctions(unittest.TestCase):
    
    def test_nyt_pass(self):
        
        test_data = """date,cases,deaths
2020-09-25,1,0
2020-09-26,1,0
2020-09-27,1,0"""
        
        test_cases, test_exceptions = extract_nyt(test_data)
        
        print('test_cases', test_cases)
        print('test_exceptions', test_exceptions)
        self.assertEqual(len(test_exceptions), 0)
        self.assertEqual(len(test_cases), 3)
    
    def test_jh_pass(self):
        
        test_data = """
        Date,Country/Region,Province/State,Lat,Long,Confirmed,Recovered,Deaths
2020-01-22,US,,40.0,-100.0,1,1,1
2020-01-23,US,,40.0,-100.0,1,1,1
2020-01-24,US,,40.0,-100.0,1,1,1
        """
        
        test_recovered, test_exceptions = extract_jh(test_data)
        print('test_recovered', test_recovered)
        print('test_exceptions', test_exceptions)
        self.assertEqual(len(test_recovered), 3)
        self.assertEqual(len(test_exceptions), 0)
    
    def test_nyt_fail(self):
        
        test_data = """
        date,cases,deaths
20w20-09-25,1,0
2020-0w9-26,1,0
2020-09-2w7,1,0
        """
        
        test_cases, test_exceptions = extract_nyt(test_data)
        
        print('test_cases', test_cases)
        print('test_exceptions', test_exceptions)
        self.assertEqual(len(test_exceptions), 5)
        self.assertEqual(len(test_cases), 0)
    
    def test_jh_fail(self):
        
        test_data = """Date,Country/Region,Province/State,Lat,Long,Confirmed,Recovered,Deaths
20w20-01-22,US,,40.0,-100.0,1,1,1
2020-0w1-23,US,,40.0,-100.0,1,1,1
2020-01-2w4,US,,40.0,-100.0,1,1,1"""
        
        test_recovered, test_exceptions = extract_jh(test_data)
        print('test_recovered', test_recovered)
        print('test_exceptions', test_exceptions)
        self.assertEqual(len(test_recovered), 0)
        self.assertEqual(len(test_exceptions), 3)

if __name__ == '__main__':
    unittest.main()