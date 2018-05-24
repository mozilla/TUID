
cd ~/TUID
export PYTHONPATH=.:vendor

rm resources/tuid_app.db

python tuid/app.py --config=resources/config/prod.json
