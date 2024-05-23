text_date() {
    echo "[$(date +"%m-%d-%Y-%H:%M:%S.%N" | cut -c -23)]"
}

if [ ! -d "logs" ]; then
    mkdir logs
fi

echo "$(text_date) PIPE3 COLLECTION STARTED" | tee "logs/PIPETHREELOG-$(date +"%m-%d").log"

git pull | tee -a "logs/PIPETHREELOG-$(date +"%m-%d").log"
echo "$(text_date) git pull complete" | tee -a "logs/PIPETHREELOG-$(date +"%m-%d").log"

pip install -r requirements.txt > /dev/null 2>&1
python pipethree.py -G -P

echo "$(text_date) Stopping pipethree.py script" | tee -a "logs/PIPETHREELOG-$(date +"%m-%d").log"
