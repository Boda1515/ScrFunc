# Orchest
import logging
from azure.durable_functions import DurableOrchestrationContext, Orchestrator
import azure.durable_functions as df


def orchestrator_function(context: df.DurableOrchestrationContext):
    # Get Data from Http starter
    input_data = context.get_input()
    start_url = input_data["start_url"]
    region = input_data["region"]
    max_pages = input_data["max_pages"]

    # # Initialize call counts For Activity Functions
    # ScraperAmazon = 0

    ###########################################################################################
    # Process ScraperAmazon   ---> Activity

    scraped_data = yield context.call_activity("ScraperAmazon", {
        "start_url": start_url,
        "region": region,
        "max_pages": max_pages
    })

    #############################################################################################
    # Log activity call counts
    # logging.info(
    #     f"AmazonLinks activity function called {ScraperAmazon} times.")

    # Return the combined results with metrics and CSV paths
    return {
        "AmazonData": {
            "scraped_data": scraped_data
        }
    }


main = Orchestrator.create(orchestrator_function)
