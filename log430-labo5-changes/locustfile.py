"""
Locustfile for Labo 07 user event load testing.
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""

import random
import uuid

from locust import HttpUser, between, task


class StoreManagerUser(HttpUser):
    """Create and delete users to produce Kafka events under load."""

    wait_time = between(0.1, 0.5)

    @task
    def create_then_delete_user(self):
        unique_id = uuid.uuid4().hex
        payload = {
            "name": f"Locust User {unique_id[:8]}",
            "email": f"locust.{unique_id}@example.com",
            "user_type_id": random.randint(1, 3),
        }

        with self.client.post(
            "/users",
            json=payload,
            name="POST /users",
            catch_response=True,
        ) as response:
            if response.status_code != 201:
                response.failure(
                    f"Expected HTTP 201, received {response.status_code}: "
                    f"{response.text}"
                )
                return

            try:
                user_id = response.json()["user_id"]
            except (ValueError, KeyError):
                response.failure("The response does not contain user_id")
                return

        with self.client.delete(
            f"/users/{user_id}",
            name="DELETE /users/[id]",
            catch_response=True,
        ) as response:
            if response.status_code != 200:
                response.failure(
                    f"Expected HTTP 200, received {response.status_code}: "
                    f"{response.text}"
                )
