import argparse
import json
import os
from datetime import datetime

import pandas as pd
import slacker


class SupportTracker:
    """A class that helps track support requests"""
    MAX_PAGE_SIZE = 100
    TOKEN_ENV_VAR = "SLACK_TOKEN"

    def __init__(self):
        # Set up Slack client
        self.client = slacker.Slacker(token=os.environ[self.TOKEN_ENV_VAR])
        self.client.auth.test()

    def get_messages(self, handle: str) -> list:
        """Get info about messages where users pinged a support handle

        :param handle: The support handle (don't include '@')

        :return: A pd.DataFrame of information about each matching message.
            Has asker_handle, timestamp, msg_text, and slack_link columns.
        """
        # The query is just the ID of the support handle
        query = self._get_usergroup_id(handle)

        this_page, last_page, messages = self._get_some_messages(
            query=query,
            page_num=1
        )

        # Scoll over messages past the first page
        while this_page != last_page:
            this_page, last_page, new_messages = self._get_some_messages(
                query=query,
                page_num=this_page + 1
            )
            messages += new_messages

        return pd.DataFrame(messages)

    def _get_some_messages(self, query: str, page_num: int) -> list:
        """Get and parse one page of messages matching a query"""
        res = self.client.search.messages(
            query=query,
            sort='timestamp',
            sort_dir='asc',
            count=self.MAX_PAGE_SIZE,
            page=page_num
        )

        this_page = res.body['messages']['pagination']['page']
        last_page = res.body['messages']['pagination']['page_count']
        messages = [
            {
                'asker_handle': msg['username'],
                'timestamp': datetime.fromtimestamp(int(float(msg['ts']))),
                'msg_text': msg['text'],
                'slack_link': msg['permalink']
            }
            for msg in res.body['messages']['matches']
            if msg['username'] != 'slackbot'
        ]

        return this_page, last_page, messages

    def _get_usergroup_id(self, handle: str) -> str:
        """Get the ID of the support handle"""
        return [
            usergroup['id']
            for usergroup in self.client.usergroups.list().body['usergroups']
            if usergroup['handle'] == handle
        ][0]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Save support request information to CSV. Your Slack token '
            'should be saved in the SLACK_TOKEN environment variable. See how '
            'to get one at https://api.slack.com/custom-integrations/legacy-tokens',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        '--support_handle',
        help="The support handle (don't include '@')"
    )
    parser.add_argument(
        '--output_path',
        help='Path to save a CSV of support request information',
        default='./support_requests.csv'
    )
    args = parser.parse_args()

    assert args.support_handle, 'Must provide the --support_handle arg'

    st = SupportTracker()
    message_df = st.get_messages(args.support_handle)
    message_df.to_csv(args.output_path, index=False)
