from unittest import TestLoader, TestSuite, TextTestRunner
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from test_downloadRasters import DownloadRastersTestCases
from test_emptyDB import EmptyDBTestCases
from test_initDB import InitDBTestCases
from test_filledDB import FilledDBTestCases

testCases = (
    DownloadRastersTestCases,
    EmptyDBTestCases,
    InitDBTestCases,
    FilledDBTestCases
)

def load_tests(loader, tests, pattern):
    suite = TestSuite()
    for testCase in testCases:
        suite.addTests(loader.loadTestsFromTestCase(testCase))
    return suite

if __name__ == "__main__":
    loader = TestLoader()
    suite = load_tests(loader, tests=None, pattern=None)
    runner = TextTestRunner()
    runner.run(suite)