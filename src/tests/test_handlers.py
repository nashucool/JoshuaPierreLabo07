"""
Tests for Coolriel
SPDX - License - Identifier: LGPL - 3.0 - or -later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
from datetime import datetime
import pytest
from handlers.user_created_handler import UserCreatedHandler
from handlers.user_deleted_handler import UserDeletedHandler


def test_user_created_handler(tmp_path):
    user_creation = UserCreatedHandler(output_dir=str(tmp_path))
    mock_event = {
        "id": 998,
        "name": 'Joanne Test',
        "email": 'joannetest@example.com',
        "datetime": str(datetime.now())
    }
    user_creation.handle(mock_event)
    output_file = tmp_path / "welcome_998.html"
    assert output_file.exists()
    assert "Joanne Test" in output_file.read_text(encoding="utf-8")

def test_user_deleted_handler(tmp_path):
    user_deletion = UserDeletedHandler(output_dir=str(tmp_path))
    mock_event = {
        "id": 999,
        "name": 'Joe Test',
        "email": 'joetest@example.com',
        "datetime": str(datetime.now())
    }
    user_deletion.handle(mock_event)
    output_file = tmp_path / "goodbye_999.html"
    assert output_file.exists()
    html = output_file.read_text(encoding="utf-8")
    assert "Joe Test" in html
    assert "joetest@example.com" in html


@pytest.mark.parametrize(
    ("user_type_id", "expected_message"),
    [
        (1, "Merci d'avoir visité notre magazin"),
        (2, "Salut et bienvenue dans l'équipe"),
        (3, "Bienvenue dans l'équipe de direction"),
    ],
)
def test_welcome_template_depends_on_user_type(
    tmp_path,
    user_type_id,
    expected_message,
):
    handler = UserCreatedHandler(output_dir=str(tmp_path))
    handler.handle({
        "id": 1000 + user_type_id,
        "name": "Test Type",
        "email": "type@example.com",
        "user_type_id": user_type_id,
        "datetime": str(datetime.now()),
    })

    html = (tmp_path / f"welcome_{1000 + user_type_id}.html").read_text(
        encoding="utf-8"
    )
    assert expected_message in html


@pytest.mark.parametrize(
    ("user_type_id", "expected_message"),
    [
        (1, "Merci d'avoir été client"),
        (2, "Merci pour ton travail au sein de l'équipe"),
        (3, "Merci pour votre leadership"),
    ],
)
def test_goodbye_template_depends_on_user_type(
    tmp_path,
    user_type_id,
    expected_message,
):
    handler = UserDeletedHandler(output_dir=str(tmp_path))
    handler.handle({
        "id": 2000 + user_type_id,
        "name": "Test Type",
        "email": "type@example.com",
        "user_type_id": user_type_id,
        "datetime": str(datetime.now()),
    })

    html = (tmp_path / f"goodbye_{2000 + user_type_id}.html").read_text(
        encoding="utf-8"
    )
    assert expected_message in html
