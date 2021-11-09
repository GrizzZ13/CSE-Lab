mr_wc_test(){
./test-lab2-part2-b.sh chfs2 | grep -q "Passed mr-wc-distributed test."
if [ $? -ne 0 ];
then
        echo "Failed test-part2-b"
else
        #exit
		ps -e | grep -q "chfs_client"
		if [ $? -ne 0 ];
		then
				echo "FATAL: chfs_client DIED!"
				exit
		else
			score=$((score+30))
			echo "Passed part2 B (Word Count with distributed MapReduce)"
			#echo $score
		fi
fi
}

mr_wc_test