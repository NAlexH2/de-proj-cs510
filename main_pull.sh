text_date() {
    echo "[$(date +"%m/%d/%Y-%H:%M:%S.%N" | cut -c -23)]"
}

echo "$(text_date) DATA COLLECTION START" >> MAINLOG
cd /home/nharris/de-proj-cs510/
echo -e "$(text_date) cd into dir complete -> $(pwd)" >> MAINLOG
git pull
echo "$(text_date) git pull complete" >> MAINLOG
echo "$(text_date) Starting python script" >> MAINLOG
python main.py -U >> MAINLOG
echo "$(text_date) DATA COLLECTION COMPLETE" >> MAINLOG


