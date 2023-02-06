from prefect import flow


@flow(log_prints=True)
def send_a_greeting():
    print("Good evening Stephen 🤗")
    print("This is another message to confirm this function works! 🤗")


if __name__ == "__main__":
    send_a_greeting()