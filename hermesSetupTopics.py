import requests
import json


def print_request(r):
    print("--------------------------------------------------")
    print('request.url: %s' % r.url)
    print('request.status_code: %s' % r.status_code)
    print('request.headers: %s' % r.headers)
    print('request.text: %s' % r.text)
    print('request.request.body: %s' % r.request.body)
    print('request.request.headers: %s' % r.request.headers)
    print("--------------------------------------------------")


hermesManagmentUrl = "http://localhost:8090"


def configureHermes():
    # Create Group moviePortal
    print_request(requests.post(
            hermesManagmentUrl + '/groups',
            data=json.dumps({
                "groupName": "moviePortal"
            }),
            headers={"Content-Type": "application/json"}
        )
    )

    # Create Topic add-genre
    print_request(requests.post(
            hermesManagmentUrl + '/topics',
            data=json.dumps({
                "name": "moviePortal.add-genre",
                "description": "This is topic responsible for added new gerne to service",
                "contentType": "JSON",
                "retentionTime": {
                    "duration": 1
                },
                "owner": {
                    "source": "Plaintext",
                    "id": "My test"
                }
            }),
            headers={"Content-Type": "application/json"}
        )
    )

    # Subscribe to Topic add-genre
    print_request(requests.post(
            hermesManagmentUrl + '/topics/moviePortal.add-genre/subscriptions',
            data=json.dumps({
                "topicName": "moviePortal.add-genre",
                "name": "ratingServiceSubscription",
                "description": "This is ratingService subscription",
                "endpoint": "http://movie-service:5000/genresUpdate",
                "owner": {
                    "source": "Plaintext",
                    "id": "My Team"
                },
                "trackingMode": "trackingAll",
                "trackingEnabled": True
            }),
            headers={"Content-Type": "application/json"}
        )
    )


if __name__ == "__main__":
    configureHermes()
