text_date() {
    echo "[$(date +"%m-%d-%Y-%H:%M:%S.%N" | cut -c -23)]"
}

echo "$(text_date) SUBSCRIBER STARTING" > "SUBLOG-$(date +"%Y-%m-%d").txt"

cd /home/nharris/de-proj-cs510/
echo -e "$(text_date) cd into dir complete -> $(pwd)" >> "SUBLOG-$(date +"%Y-%m-%d").txt"

git pull >> "SUBLOG-$(date +"%Y-%m-%d").txt"

echo "$(text_date) git pull complete" >> "SUBLOG-$(date +"%Y-%m-%d").txt"

echo "$(text_date) Starting subscriber listener." >> "SUBLOG-$(date +"%Y-%m-%d").txt"

python subscriber.py >> "SUBLOG-$(date +"%Y-%m-%d").txt"
