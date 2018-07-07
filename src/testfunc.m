function sum1=testfunc(season_var,badyear,MUTRAT_mean,MUTRAT_spread,MUTRAT_frac, Up,Spread,Frac,competition,mu_winter,space, lnr)
sum1 = season_var+badyear;
fileNew=fopen(sprintf('meanresults%d.txt', lnr),'w');
fprintf(fileNew, '%f \n', sum1);
fclose(fileNew);
fileNew=fopen(sprintf('popdata%d.txt', lnr),'w');
fprintf(fileNew, '%f \n', sum1);
fclose(fileNew);
fileNew=fopen(sprintf('timedata%d.txt', lnr),'w');
fprintf(fileNew, '%f \n', sum1);
fclose(fileNew);
end