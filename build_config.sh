echo The username is ${{ secrets.SF_WAREHOUSE }}
sed -i 's,SF_ACCOUNT_DEV'${{ secrets.SF_WAREHOUSE }}',g' build_config.sh