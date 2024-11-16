import os
import secrets
from functools import wraps
from flask import Flask, request, jsonify, render_template_string, make_response, redirect, url_for
from pathlib import Path

app = None

LOGIN = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Login</title>
</head>
<body>
    <h1>Login</h1>
    {% if error %}
        <p style="color: red;">{{ error }}</p>
    {% endif %}
    <form method="POST">
        <label for="token">API Token:</label>
        <input type="password" id="token" name="token" required>
        <button type="submit">Login</button>
    </form>
</body>
</html>'''

def generate_token():
    return secrets.token_hex(16)

def save_token(token, config_dir):
    config_dir.mkdir(parents=True, exist_ok=True)
    with open(config_dir / "token", "w") as f:
        f.write(token)

def load_token(config_dir):
    token_file = config_dir / "token"
    if token_file.exists():
        with open(token_file, "r") as f:
            return f.read().strip()
    return None

def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.cookies.get('api_token')
        if not token:
            return redirect(url_for('login'))
        if token != app.config["API_TOKEN"]:
            return jsonify({"message": "Invalid token"}), 401
        return f(*args, **kwargs)
    return decorated

def _setup_routes(app, socketio):
    @app.route('/login', methods=['GET', 'POST'])
    def login():
        if request.method == 'POST':
            token = request.form.get('token')
            if token == app.config["API_TOKEN"]:
                response = make_response(redirect('/'))
                # AI-TODO: set cookie to forever. also make it HTTP only
                response.set_cookie('api_token', token,
                                    httponly=True, expires=2147483647)
                return response
            else:
                return render_template_string(LOGIN, error="Invalid token")
        return render_template_string(LOGIN)

    @socketio.on('connect')
    def connect_handler():
        token = request.cookies.get('api_token')
        if not token:
            return False
        if token != app.config["API_TOKEN"]:
            return False

    
def install(app, socketio, app_name):
    globals()['app'] = app
    config_dir = Path.home() / ".config" / app_name
    token = load_token(config_dir)
    if not token:
        token = generate_token()
        save_token(token, config_dir)
    
    app.config["API_TOKEN"] = token
    print(f"API Token: {token}")
    print("Please use this token to log in via the /login route.")

    _setup_routes(app, socketio)

if __name__ == "__main__":
    app = Flask(__name__)
    initialize_auth(app, "your_app_name")
    app.run()
