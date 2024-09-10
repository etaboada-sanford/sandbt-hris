import subprocess

def list_installed_packages():
    result = subprocess.run(['pip', 'list'], capture_output=True, text=True)
    print(result.stdout)