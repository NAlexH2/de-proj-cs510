text_date() {
    echo "[$(date +"%m-%d-%Y-%H:%M:%S.%N" | cut -c -23)]"
}

echo "$(text_date) DATA COLLECTION START" >> "MAINLOG-$(date +"%Y-%m-%d").txt"
cd /home/nharris/de-proj-cs510/
echo -e "$(text_date) cd into dir complete -> $(pwd)" >> "MAINLOG-$(date +"%Y-%m-%d").txt"
git pull >> "SUBLOG-$(date +"%Y-%m-%d").txt"
echo "$(text_date) git pull complete" >> "MAINLOG-$(date +"%Y-%m-%d").txt"
echo "$(text_date) Starting python script" >> "MAINLOG-$(date +"%Y-%m-%d").txt"
python main.py -U -P >> "MAINLOG-$(date +"%Y-%m-%d").txt"
echo "$(text_date) DATA COLLECTION COMPLETE" >> "MAINLOG-$(date +"%Y-%m-%d").txt"


