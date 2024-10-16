from unittest import TestLoader, TestSuite, TextTestRunner
from tests.test_downloadRasters import DownloadRastersTestCases
from tests.test_emptyDB import EmptyDBTestCases
from tests.test_initDB import InitDBTestCases
from tests.test_filledDB import FilledDBTestCases

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