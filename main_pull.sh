text_date="[$(date +"%m/%d/%Y-%H:%M:%S")]"

echo "$(text_date) DATA COLLECTION START" >> MAINLOG
date >> MAINLOG
cd /home/nharris/de-proj-cs510/
echo -e "$(text_date) cd into dir complete -> $(pwd)" >> MAINLOG
git pull
echo "$(text_date) git pull complete" >> MAINLOG
echo "$(text_date) Starting python script" >> MAINLOG
python main.py -U >> MAINLOG
echo "$(text_date) DATA COLLECTION COMPLETE" >> MAINLOG
