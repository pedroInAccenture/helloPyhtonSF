echo replacing var ... 
ls -l
sed -i "s/SF_ACCOUNT_DEV/"${{ secrets.SF_ACCOUNT }}"/" config.ini
sed -i "s/SF_USER_DEV/"${{ secrets.SF_USER }}"/" config.ini
sed -i "s/SF_PASS_DEV/"${{ secrets.SF_PASS }}"/" config.ini
sed -i "s/SF_WAREHOUSE_DEV/"${{ secrets.SF_ACCOUNT }}"/" config.ini
sed -i "s/SF_DATABASE_DEV/"${{ secrets.SF_DATABASE }}"/" config.ini
sed -i "s/SF_SCHEMA_DEV/"${{ secrets.SF_SCHEMA }}"/" config.ini
cat config.ini
echo End script
