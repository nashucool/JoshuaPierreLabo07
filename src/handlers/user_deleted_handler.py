"""
Handler: User Deleted
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""

import os
from pathlib import Path
from handlers.base import EventHandler
from typing import Dict, Any

class UserDeletedHandler(EventHandler):
    """Handles UserDeleted events"""

    TEMPLATE_BY_USER_TYPE = {
        1: "goodbye_client_template.html",
        2: "goodbye_employee_template.html",
        3: "goodbye_manager_template.html",
    }
    
    def __init__(self, output_dir: str = "output"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        super().__init__()
    
    def get_event_type(self) -> str:
        """Return the event type this handler processes"""
        return "UserDeleted"
    
    def handle(self, event_data: Dict[str, Any]) -> None:
        """Create an HTML email based on user deletion data"""
        user_id = event_data.get("id")
        name = event_data.get("name")
        email = event_data.get("email")
        deletion_date = event_data.get("datetime")
        user_type_id = int(event_data.get("user_type_id", 1))

        template_name = self.TEMPLATE_BY_USER_TYPE.get(
            user_type_id,
            self.TEMPLATE_BY_USER_TYPE[1],
        )
        project_root = Path(__file__).parent.parent
        template_path = project_root / "templates" / template_name
        with open(template_path, "r", encoding="utf-8") as file:
            html_content = file.read()

        html_content = html_content.replace("{{user_id}}", str(user_id))
        html_content = html_content.replace("{{name}}", name)
        html_content = html_content.replace("{{email}}", email)
        html_content = html_content.replace("{{deletion_date}}", deletion_date)

        filename = os.path.join(self.output_dir, f"goodbye_{user_id}.html")
        with open(filename, "w", encoding="utf-8") as file:
            file.write(html_content)

        self.logger.debug(
            f"Courriel HTML généré à {name} (ID: {user_id}), {filename}"
        )
