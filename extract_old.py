
import subprocess

with open("app_old.py", "wb") as f:
    subprocess.run(["git", "show", "82186ca:app.py"], stdout=f)
