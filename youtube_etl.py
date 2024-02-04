# -*- coding: utf-8 -*-
import os
import csv
import boto3
import googleapiclient.discovery

def main(youtube):
    video_id = "KerNf0NANMo"

    request = youtube.commentThreads().list(
        part="snippet,replies",
        videoId=video_id
    )
    response = request.execute()

    comments_list = process_comments(response.get("items", []))

    while response.get("nextPageToken"):
        request = youtube.commentThreads().list(
            part="snippet,replies",
            videoId=video_id,
            pageToken=response["nextPageToken"]
        )
        response = request.execute()
        comments_list.extend(process_comments(response.get("items", [])))

    # Save comments to a CSV file
    csv_filename = "demo_youtube_etl.csv"
    save_to_csv(comments_list, csv_filename)

    # Upload CSV to S3
    s3_bucket_name = "youtube-etl-airflow1"
    s3_object_key = "demo_youtube_etl.csv"
    upload_to_s3(csv_filename, s3_bucket_name, s3_object_key)

    print(f"Total comments processed: {len(comments_list)}")
    print(f"Comments saved to {csv_filename}")
    print(f"CSV file uploaded to S3 bucket: {s3_bucket_name}, object key: {s3_object_key}")


def process_comments(response_items):
    comments = []
    for comment in response_items:
        author = comment.get("snippet", {}).get("topLevelComment", {}).get("snippet", {}).get("authorDisplayName", "")
        comment_text = comment.get("snippet", {}).get("topLevelComment", {}).get("snippet", {}).get("textOriginal", "")
        publish_time = comment.get("snippet", {}).get("topLevelComment", {}).get("snippet", {}).get("publishedAt", "")
        comment_info = {"author": author, "comment": comment_text, "published_at": publish_time}
        comments.append(comment_info)

    print(f"Finished processing {len(comments)} comments.")
    return comments


def save_to_csv(data, filename):
    # Save data to a CSV file
    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["author", "comment", "published_at"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow(row)


def upload_to_s3(local_file, bucket_name, s3_key):
    s3 = boto3.client("s3")
    s3.upload_file(local_file, bucket_name, s3_key)


if __name__ == "__main__":
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"
    DEVELOPER_KEY = "AIzaSyCni9RMszvm-3VPvLp22roMckE-zXEvSxc"

    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=DEVELOPER_KEY)

    main(youtube)
