text_date() {
    echo "[$(date +"%m/%d/%Y-%H:%M:%S.%N" | cut -c -23)]"
}

echo "$(text_date) SUBSCRIBER ACK STARTING" >> SUBLOG
cd /home/nharris/de-proj-cs510/
echo -e "$(text_date) cd into dir complete -> $(pwd)" >> SUBLOG
git pull
echo "$(text_date) git pull complete" >> SUBLOG
echo "$(text_date) Starting subscriber listener." >> SUBLOG
echo "$(text_date) THIS IS GOING TO RUN AS LONG AS THE VM IS UP." >> SUBLOG
python subsciber.py >> SUBLOG
