import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
import os
from datetime import datetime
import sys

# Add the project root to the Python path to allow for absolute imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.data_operations.WorkFlowManager import WorkFlowManager

class TestWorkFlowManager(unittest.TestCase):

    def setUp(self):
        """Set up test fixtures, if any."""
        # We will use the 'test' mode for most tests to prevent actual API calls
        self.workflow_manager = WorkFlowManager(mode='test')

    @patch('src.data_operations.WorkFlowManager.DatabaseManager')
    @patch('src.data_operations.WorkFlowManager.DataProcessor')
    @patch('src.data_operations.WorkFlowManager.DataFetcher')
    def test_update_bills_data(self, MockDataFetcher, MockDataProcessor, MockDatabaseManager):
        """Test case for the update_bills_data method."""
        print("Testing update_bills_data...")

        # Configure the mocks
        mock_db_manager = MockDatabaseManager.return_value
        mock_db_manager.get_latest_propose_date.return_value = '2025-01-01'

        mock_fetcher = MockDataFetcher.return_value
        mock_fetcher.fetch_data.return_value = pd.DataFrame({
            'BILL_ID': ['1', '2'],
            'PROPOSE_DT': ['2025-01-02', '2025-01-03']
        })

        mock_processor = MockDataProcessor.return_value
        # Simulate returning a non-empty dataframe to continue execution
        mock_processor.remove_duplicates.return_value = pd.DataFrame({
            'BILL_ID': ['1', '2'],
            'PROPOSE_DT': ['2025-01-02', '2025-01-03']
        })
        # Mock other processor methods to return empty dataframes for simplicity
        mock_processor.process_congressman_bills.return_value = pd.DataFrame({'BILL_ID': ['1', '2']})
        mock_processor.process_chairman_bills.return_value = (pd.DataFrame(), pd.DataFrame())
        mock_processor.process_gov_bills.return_value = pd.DataFrame()

        # Call the method to be tested
        result_df = self.workflow_manager.update_bills_data()

        # Assertions
        self.assertIsNotNone(result_df)
        self.assertIsInstance(result_df, pd.DataFrame)
        MockDatabaseManager.assert_called_once()
        MockDataFetcher.assert_called_once()
        MockDataProcessor.assert_called_once()
        mock_fetcher.fetch_data.assert_called_with('bills')
        mock_processor.remove_duplicates.assert_called_once()
        print("update_bills_data test passed.")

    @patch('src.data_operations.WorkFlowManager.APISender')
    @patch('src.data_operations.WorkFlowManager.DataFetcher')
    def test_update_lawmakers_data(self, MockDataFetcher, MockAPISender):
        """Test case for the update_lawmakers_data method."""
        print("Testing update_lawmakers_data...")

        # Configure the mocks
        mock_fetcher = MockDataFetcher.return_value
        mock_fetcher.fetch_data.return_value = pd.DataFrame({
            'MONA_CD': ['test_id'], 'HG_NM': ['test_name'], 'CMIT_NM': ['test_commit'],
            'POLY_NM': ['test_party'], 'REELE_GBN_NM': ['test_elected'], 'HOMEPAGE': ['test_homepage'],
            'ORIG_NM': ['test_district'], 'UNITS': ['초선'], 'BTH_DATE': ['1990-01-01'],
            'SEX_GBN_NM': ['남'], 'E_MAIL': ['test@example.com'], 'ASSEM_ADDR': ['test_address'],
            'TEL_NO': ['010-1234-5678'], 'MEM_TITLE': ['test_history'], 'ENG_NM': [''],
            'HJ_NM': [''], 'BTH_GBN_NM': [''], 'ELECT_GBN_NM': [''], 'STAFF': [''],
            'CMITS': [''], 'SECRETARY': [''], 'SECRETARY2': [''], 'JOB_RES_NM': [''],
        })
        mock_sender = MockAPISender.return_value

        # Call the method
        result_df = self.workflow_manager.update_lawmakers_data()

        # Assertions
        self.assertIsNotNone(result_df)
        self.assertIsInstance(result_df, pd.DataFrame)
        self.assertIn('congressmanId', result_df.columns)
        MockDataFetcher.assert_called_once()
        mock_fetcher.fetch_data.assert_called_with('lawmakers')
        mock_sender.send_data.assert_not_called()
        print("update_lawmakers_data test passed.")

    @patch('src.data_operations.WorkFlowManager.APISender')
    @patch('src.data_operations.WorkFlowManager.DatabaseManager')
    @patch('src.data_operations.WorkFlowManager.DataFetcher')
    def test_update_bills_timeline(self, MockDataFetcher, MockDatabaseManager, MockAPISender):
        """Test case for the update_bills_timeline method."""
        print("Testing update_bills_timeline...")

        # Configure mocks
        mock_db_manager = MockDatabaseManager.return_value
        mock_db_manager.get_latest_timeline_date.return_value = datetime(2025, 1, 1)
        mock_fetcher = MockDataFetcher.return_value
        mock_fetcher.fetch_data.return_value = pd.DataFrame({
            'DT': ['2025-01-02'], 'BILL_ID': ['3'], 'STAGE': ['test_stage'], 'COMMITTEE': ['test_committee']
        })
        mock_sender = MockAPISender.return_value

        # Call method
        result_df = self.workflow_manager.update_bills_timeline()

        # Assertions
        self.assertIsNotNone(result_df)
        self.assertIsInstance(result_df, pd.DataFrame)
        self.assertIn('statusUpdateDate', result_df.columns)
        MockDataFetcher.assert_called_once()
        mock_fetcher.fetch_data.assert_called_with('bill_timeline')
        mock_sender.send_data.assert_not_called()
        print("update_bills_timeline test passed.")

    @patch('src.data_operations.WorkFlowManager.APISender')
    @patch('src.data_operations.WorkFlowManager.DataFetcher')
    def test_update_bills_result(self, MockDataFetcher, MockAPISender):
        """Test case for the update_bills_result method."""
        print("Testing update_bills_result...")

        # Configure mocks
        mock_fetcher = MockDataFetcher.return_value
        mock_fetcher.fetch_data.return_value = pd.DataFrame({
            'BILL_ID': ['4'], 'PROC_RESULT_CD': ['test_result']
        })
        mock_sender = MockAPISender.return_value

        # Call method
        result_df = self.workflow_manager.update_bills_result()

        # Assertions
        self.assertIsNotNone(result_df)
        self.assertIsInstance(result_df, pd.DataFrame)
        self.assertIn('billId', result_df.columns)
        MockDataFetcher.assert_called_once()
        mock_fetcher.fetch_data.assert_called_with('bill_result')
        mock_sender.send_data.assert_not_called()
        print("update_bills_result test passed.")

    @patch('src.data_operations.WorkFlowManager.APISender')
    @patch('src.data_operations.WorkFlowManager.DataFetcher')
    def test_update_bills_vote(self, MockDataFetcher, MockAPISender):
        """Test case for the update_bills_vote method."""
        print("Testing update_bills_vote...")

        # Configure mocks
        mock_fetcher = MockDataFetcher.return_value
        mock_fetcher.fetch_data.side_effect = [
            pd.DataFrame({
                'BILL_ID': ['5'], 'VOTE_TCNT': [100], 'YES_TCNT': [80],
                'NO_TCNT': [10], 'BLANK_TCNT': [10]
            }),
            pd.DataFrame({
                'BILL_ID': ['5'], 'POLY_NM': ['party_a'], 'YES_TCNT': [40]
            })
        ]
        mock_sender = MockAPISender.return_value

        # Call method
        df_vote, df_vote_party = self.workflow_manager.update_bills_vote()

        # Assertions
        self.assertIsNotNone(df_vote)
        self.assertIsInstance(df_vote, pd.DataFrame)
        self.assertIn('totalVoteCount', df_vote.columns)
        self.assertIsNotNone(df_vote_party)
        self.assertIsInstance(df_vote_party, pd.DataFrame)
        self.assertEqual(mock_fetcher.fetch_data.call_count, 2)
        mock_sender.send_data.assert_not_called()
        print("update_bills_vote test passed.")

    @patch('requests.get')
    @patch('src.data_operations.WorkFlowManager.APISender')
    @patch('src.data_operations.WorkFlowManager.DataFetcher')
    def test_update_bills_alternatives(self, MockDataFetcher, MockAPISender, mock_requests_get):
        """Test case for the update_bills_alternatives method."""
        print("Testing update_bills_alternatives...")

        # Configure mocks for requests.get
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = '<response><body><items><item><proposeDt>2025-01-01</proposeDt><billId>alt-1</billId><proposerKind>대안</proposerKind></item></items></body></response>'.encode('utf-8')
        
        # To stop the while loop in the method
        mock_empty_response = MagicMock()
        mock_empty_response.status_code = 200
        mock_empty_response.content = b'<response><body><items></items></body></response>'
        
        mock_requests_get.side_effect = [mock_response, mock_empty_response]

        mock_fetcher = MockDataFetcher.return_value
        mock_fetcher.fetch_bills_alternatives.return_value = pd.DataFrame({
            'alternative_bill_id': ['alt-1'], 'original_bill_id': ['orig-1']
        })
        mock_sender = MockAPISender.return_value

        # Call method
        result_df = self.workflow_manager.update_bills_alternatives()

        # Assertions
        self.assertIsNotNone(result_df)
        self.assertIsInstance(result_df, pd.DataFrame)
        self.assertGreaterEqual(mock_requests_get.call_count, 1)
        MockDataFetcher.assert_called_once()
        mock_fetcher.fetch_bills_alternatives.assert_called_once()
        mock_sender.send_data.assert_not_called()
        print("update_bills_alternatives test passed.")

if __name__ == '__main__':
    # Note: Running this file directly might require additional environment setup (e.g., .env file).
    # It is recommended to run tests using a test runner like pytest.
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
