"""
1. Add widget Dashboard Script
2. Add widget Script
3. Update Dashboard Script
4. Update Widget Script
5. Replace Dashboard Script by matching string from existing script
6. Replace Widget Script by matching string from existing script
7. Add users to share
8. Republish Dashboard
9. Export Dashboard
10. Export Widget
11. Change Dashboard Owner
12. Export widget/dashboard as csv
13. Get all dashboards
14. Get all tables AND columns used in dashboard
"""

from sisensepy.api_client import APIClient


class Dashboard:
    def __init__(self, api_client=None, config_file="config.yaml", debug=False):
        """
        Initializes the Dashboard class.

        If no API client is provided, it will create an APIClient internally using the provided config file.

        Parameters:
            api_client (APIClient, optional): An existing APIClient instance. If None, a new APIClient is created.
            config_file (str): Path to the YAML configuration file. Default is 'config.yaml'.
        """
        # If an API client is provided, use it. Otherwise, initialize a new APIClient with the config file.
        if api_client:
            self.api_client = api_client
        else:
            self.api_client = APIClient(config_file=config_file, debug=debug)

        # Use the logger from the APIClient instance
        self.logger = self.api_client.logger

    def get_all_dashboards(self):
        """
        Retrieves all dashboards from the Sisense server.

        Parameters: None

        Returns: 
        """

        return