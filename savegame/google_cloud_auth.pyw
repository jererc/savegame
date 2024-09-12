from google_cloud import GoogleCloud


def main():
    GoogleCloud().get_oauth_creds(interact=True)


if __name__ == '__main__':
    main()
