Search.setIndex({docnames:["api","executor","index","internal","quick_start","task"],envversion:{"sphinx.domains.c":1,"sphinx.domains.changeset":1,"sphinx.domains.citation":1,"sphinx.domains.cpp":1,"sphinx.domains.index":1,"sphinx.domains.javascript":1,"sphinx.domains.math":2,"sphinx.domains.python":1,"sphinx.domains.rst":1,"sphinx.domains.std":1,sphinx:56},filenames:["api.rst","executor.ipynb","index.rst","internal.ipynb","quick_start.ipynb","task.ipynb"],objects:{"flowsaber.core":{base:[0,0,0,"-"],channel:[0,0,0,"-"],context:[0,0,0,"-"],flow:[0,0,0,"-"],operators:[0,0,0,"-"],task:[0,0,0,"-"]},"flowsaber.core.base":{Component:[0,1,1,""],ComponentCallError:[0,3,1,""],ComponentExecuteError:[0,3,1,""],aenter_context:[0,4,1,""],enter_context:[0,4,1,""]},"flowsaber.core.base.Component":{State:[0,1,1,""],call_initialize:[0,2,1,""],config:[0,2,1,""],get_full_name:[0,2,1,""],initialize_context:[0,2,1,""],start:[0,2,1,""]},"flowsaber.core.channel":{AsyncChannel:[0,1,1,""],Channel:[0,1,1,""],ChannelBase:[0,1,1,""],ConstantChannel:[0,1,1,""],ConstantQueue:[0,1,1,""],Consumer:[0,1,1,""],EventChannel:[0,1,1,""],EventChannelCheckError:[0,3,1,""],Fetcher:[0,1,1,""],LazyAsyncQueue:[0,1,1,""]},"flowsaber.core.channel.ChannelBase":{branch:[0,2,1,""],collect:[0,2,1,""],concat:[0,2,1,""],distinct:[0,2,1,""],filter:[0,2,1,""],first:[0,2,1,""],flatten:[0,2,1,""],from_list:[0,2,1,""],get:[0,2,1,""],getitem:[0,2,1,""],group:[0,2,1,""],last:[0,2,1,""],map:[0,2,1,""],merge:[0,2,1,""],mix:[0,2,1,""],reduce:[0,2,1,""],sample:[0,2,1,""],select:[0,2,1,""],split:[0,2,1,""],subscribe:[0,2,1,""],take:[0,2,1,""],unique:[0,2,1,""],until:[0,2,1,""],value:[0,2,1,""],values:[0,2,1,""],view:[0,2,1,""]},"flowsaber.core.context":{FlowSaberContext:[0,1,1,""],inject_context_attrs:[0,4,1,""]},"flowsaber.core.context.FlowSaberContext":{cache:[0,2,1,""],executor:[0,2,1,""],lock:[0,2,1,""],logger:[0,2,1,""]},"flowsaber.core.engine":{flow_runner:[0,0,0,"-"],runner:[0,0,0,"-"],task_runner:[0,0,0,"-"]},"flowsaber.core.engine.flow_runner":{FlowRunner:[0,1,1,""]},"flowsaber.core.engine.runner":{RunException:[0,3,1,""],Runner:[0,1,1,""],RunnerExecuteError:[0,3,1,""],RunnerExecutor:[0,1,1,""],call_state_change_handlers:[0,4,1,""],catch_to_failure:[0,4,1,""],run_timeout_signal:[0,4,1,""],run_timeout_thread:[0,4,1,""]},"flowsaber.core.engine.runner.Runner":{handle_state_change:[0,2,1,""],send_logs:[0,2,1,""],update_run_state:[0,2,1,""]},"flowsaber.core.engine.runner.RunnerExecutor":{DoneException:[0,3,1,""],join:[0,2,1,""],main_loop:[0,2,1,""],run:[0,2,1,""],start:[0,2,1,""]},"flowsaber.core.engine.task_runner":{TaskRunner:[0,1,1,""]},"flowsaber.core.engine.task_runner.TaskRunner":{run_task_timeout:[0,2,1,""]},"flowsaber.core.flow":{Flow:[0,1,1,""]},"flowsaber.core.flow.Flow":{call_initialize:[0,2,1,""],initialize_context:[0,2,1,""],start_execute:[0,2,1,""]},"flowsaber.core.operators":{"var":[0,5,1,""],Branch:[0,1,1,""],Collect:[0,1,1,""],Concat:[0,1,1,""],Count:[0,1,1,""],Distinct:[0,1,1,""],Filter:[0,1,1,""],First:[0,1,1,""],Flatten:[0,1,1,""],Get:[0,1,1,""],GetItem:[0,1,1,""],Group:[0,1,1,""],Last:[0,1,1,""],Map:[0,1,1,""],Max:[0,1,1,""],Merge:[0,1,1,""],Min:[0,1,1,""],Mix:[0,1,1,""],Operator:[0,1,1,""],Reduce:[0,1,1,""],Sample:[0,1,1,""],Select:[0,1,1,""],Split:[0,1,1,""],Subscribe:[0,1,1,""],Sum:[0,1,1,""],Take:[0,1,1,""],Unique:[0,1,1,""],Until:[0,1,1,""],View:[0,1,1,""]},"flowsaber.core.operators.Branch":{handle_consumer:[0,2,1,""]},"flowsaber.core.operators.Collect":{handle_consumer:[0,2,1,""]},"flowsaber.core.operators.Concat":{handle_consumer:[0,2,1,""]},"flowsaber.core.operators.Filter":{handle_input:[0,2,1,""]},"flowsaber.core.operators.Flatten":{handle_input:[0,2,1,""]},"flowsaber.core.operators.GetItem":{handle_input:[0,2,1,""]},"flowsaber.core.operators.Group":{handle_input:[0,2,1,""]},"flowsaber.core.operators.Last":{handle_input:[0,2,1,""]},"flowsaber.core.operators.Map":{handle_input:[0,2,1,""]},"flowsaber.core.operators.Mix":{handle_consumer:[0,2,1,""]},"flowsaber.core.operators.Operator":{initialize_context:[0,2,1,""]},"flowsaber.core.operators.Reduce":{handle_input:[0,2,1,""]},"flowsaber.core.operators.Sample":{handle_consumer:[0,2,1,""]},"flowsaber.core.operators.Subscribe":{handle_consumer:[0,2,1,""]},"flowsaber.core.operators.Take":{handle_input:[0,2,1,""]},"flowsaber.core.operators.Until":{handle_input:[0,2,1,""]},"flowsaber.core.task":{BaseTask:[0,1,1,""],Edge:[0,1,1,""],RunDataFileNotFoundError:[0,3,1,""],RunDataTypeError:[0,3,1,""],RunTask:[0,1,1,""],Task:[0,1,1,""]},"flowsaber.core.task.BaseTask":{enqueue_res:[0,2,1,""],handle_consumer:[0,2,1,""],handle_input:[0,2,1,""],initialize_context:[0,2,1,""],initialize_input:[0,2,1,""],initialize_output:[0,2,1,""]},"flowsaber.core.task.RunTask":{call_run:[0,2,1,""],check_run_data:[0,2,1,""],create_run_data:[0,2,1,""],handle_consumer:[0,2,1,""],handle_run_data:[0,2,1,""],initialize_context:[0,2,1,""],run:[0,2,1,""]},"flowsaber.core.task.Task":{call_run:[0,2,1,""],clean:[0,2,1,""],handle_res:[0,2,1,""],initialize_context:[0,2,1,""],need_skip:[0,2,1,""],skip:[0,2,1,""],task_hash:[0,2,1,""],task_workdir:[0,2,1,""]},"flowsaber.core.utility":{cache:[0,0,0,"-"],executor:[0,0,0,"-"],state:[0,0,0,"-"],target:[0,0,0,"-"]},"flowsaber.core.utility.cache":{Cache:[0,1,1,""],CacheInvalidError:[0,3,1,""],LocalCache:[0,1,1,""]},"flowsaber.core.utility.cache.LocalCache":{persist:[0,2,1,""]},"flowsaber.core.utility.executor":{DaskExecutor:[0,1,1,""],Executor:[0,1,1,""],Local:[0,1,1,""]},"flowsaber.core.utility.executor.DaskExecutor":{run:[0,2,1,""]},"flowsaber.core.utility.state":{Cached:[0,1,1,""],Cancelled:[0,1,1,""],Cancelling:[0,1,1,""],Done:[0,1,1,""],Drop:[0,1,1,""],Failure:[0,1,1,""],Pending:[0,1,1,""],Retrying:[0,1,1,""],Running:[0,1,1,""],Scheduled:[0,1,1,""],Skip:[0,1,1,""],State:[0,1,1,""],Success:[0,1,1,""]},"flowsaber.core.utility.target":{End:[0,1,1,""],File:[0,1,1,""],Stdin:[0,1,1,""],Stdout:[0,1,1,""],Target:[0,1,1,""]},"flowsaber.server.database":{models:[0,0,0,"-"]},"flowsaber.tasks":{shell:[0,0,0,"-"]},"flowsaber.tasks.shell":{CommandTask:[0,1,1,""],CommandTaskComposeError:[0,3,1,""],ShellFlow:[0,1,1,""],ShellTask:[0,1,1,""],ShellTaskExecuteError:[0,3,1,""]},"flowsaber.tasks.shell.CommandTask":{command:[0,2,1,""],run:[0,2,1,""]},"flowsaber.tasks.shell.ShellTask":{execute_shell_command:[0,2,1,""],get_publish_dirs:[0,2,1,""],glob_output_files:[0,2,1,""],run:[0,2,1,""]},flowsaber:{cli:[0,0,0,"-"]}},objnames:{"0":["py","module","Python module"],"1":["py","class","Python class"],"2":["py","method","Python method"],"3":["py","exception","Python exception"],"4":["py","function","Python function"],"5":["py","attribute","Python attribute"]},objtypes:{"0":"py:module","1":"py:class","2":"py:method","3":"py:exception","4":"py:function","5":"py:attribute"},terms:{"case":0,"class":[0,5],"default":0,"final":0,"float":0,"function":[0,5],"int":0,"new":[0,3],"return":[0,3],"static":0,"true":0,"var":0,"while":0,And:0,For:[0,3,4],The:[0,1,3,4],There:5,Use:0,Used:0,Using:0,__aenter__:0,__aexit__:0,__anext__:0,__annotation__:3,__code__:3,__getitem__:0,__name__:0,__next__:0,_base:0,_input:0,_output:0,_run:0,about:0,absolut:0,accept:[0,5],accord:0,accum_resourc:0,across:0,activ:[0,5],actual:[0,4],adapt:0,adapt_kwarg:0,add:0,added:5,addit:[0,5],address:0,addtion:0,aenter_context:0,after:[0,4],agent_id:0,aim:0,algorithm:0,alia:0,aliv:0,all:[0,1,3,4,5],alreadi:0,also:[0,4],altern:0,alwai:[0,5],anc:0,ani:[0,4],annot:0,anoth:0,anytim:0,anywher:0,api:2,appear:0,arerun:1,arg:0,argument:[0,4,5],arrang:0,assign:0,async:0,asyncchannel:0,asynchron:0,asyncio:0,attempt:0,attr:0,attribut:0,automat:0,avoid:3,await:0,bam:0,bam_fil:0,base:[2,5],basetask:[0,5],bash:[0,5],basic:4,been:[0,4,5],befor:[0,5],being:0,below:4,berun:5,besidesrun:1,between:0,blob:0,block:[0,5],bool:0,borrow:0,both:0,bottom:0,boundargu:0,branch:0,buffer:0,build:4,built:4,bwa:0,cach:[2,3],cache_typ:0,cacheinvaliderror:0,call:[0,3,4],call_initi:0,call_run:0,call_state_change_handl:0,callabl:0,callback:0,calle:0,can:[0,3,4,5],cancel:0,captur:0,carefulli:1,cat:0,catch_to_failur:0,caus:0,ch1:[3,5],ch2:[3,5],ch3:[3,5],chang:0,channel:[2,3,5],channelbas:0,check:[0,1,4],check_run_data:0,child:0,classmethod:0,classnam:3,clean:0,cli:2,client:0,client_kwarg:0,cloud:1,cloudpickleseri:0,cloudprovid:0,cluster:0,cluster_class:0,cluster_kwarg:0,cmd:0,code:[0,5],collect:[0,4],collect_fil:0,com:[0,4],combin:4,come:[0,4],command:[0,5],commandtask:0,commandtaskcomposeerror:0,commnd:5,compar:[0,5],compet:3,complic:0,compon:[0,4],componentcallerror:0,componentexecuteerror:0,compos:0,comput:[0,5],concat:0,concaten:0,concept:2,concurr:0,conda:5,config:0,config_dict:[0,3],configur:[0,1,4,5],configured_publish_dir:0,conflict:3,connect:0,constantchannel:0,constantqueu:0,constructor:0,consum:[0,4],content:2,context:2,context_attr:0,contextmanag:0,continu:0,control:[0,4],convers:0,convert:0,copi:0,core:0,coroutin:0,correspond:0,corrupt:0,cost:5,count:[0,4],cover:4,cpu:0,creat:[0,5],create_queu:0,create_run_data:0,create_task:0,creation:5,cur_stat:0,cur_task:0,current:0,dask:0,dask_cloudprovid:0,dask_kubernet:0,daskexecutor:0,data:[0,4],deadlock:0,debug:[0,1],decid:0,decor:0,def:0,defin:[0,4,5],depend:[0,3,4],descript:4,design:0,detail:1,dict:0,dictionari:0,differ:[0,1,3,4],dir:0,directli:0,directori:[0,3],disk:0,dispatch:0,distinct:0,distribut:0,doc:0,docstr:0,document:[1,4],doe:0,don:0,done:0,doneexcept:0,down:0,drop:0,due:[0,4,5],dump:0,duplic:0,dure:0,each:[0,3],echo:0,edg:0,edit:0,either:0,element:0,ellipsi:0,elsewher:0,emit:0,empti:0,enabl:0,end:0,endlessli:0,engin:0,enqueu:[0,4],enqueue_r:0,enter:0,enter_context:0,enumer:0,env:0,envent:0,enviro:5,envtask:2,equal:[0,3],error:0,even:[0,1,5],event:[0,1,5],eventchannel:0,eventchannelcheckerror:0,eventloop:5,exampl:[0,4],except:0,execut:[0,4,5],execute_shell_command:0,executor:[2,5],executor_typ:0,exist:0,expect:0,expos:[0,4],extra:0,extra_context:0,factori:0,failur:0,fals:0,familiar:4,fargateclust:0,fasta:0,featur:[0,4],ferch:0,fetch:0,fetcher:0,file:0,filter:0,first:0,flatten:[0,4,5],flow1:0,flow2:0,flow3:0,flow:2,flow_config:0,flow_full_nam:0,flow_id:0,flow_label:0,flow_nam:0,flow_runn:2,flow_workdir:0,flowrun:0,flowrun_nam:0,flowrunn:0,flowsab:[0,4],flowsabercontext:0,form:0,forward:0,found:0,four:5,fraction:0,freeli:0,fresh:0,from:[0,4,5],from_list:0,fstring:0,full:0,func:0,further:[0,5],furthermor:0,futur:0,gener:[0,3],get:[0,3],get_full_nam:0,get_nowait:0,get_publish_dir:0,getitem:0,git:4,github:[0,4],glob:0,glob_output_fil:0,global:0,group:0,grouped_data_tupl:0,handl:[0,5],handle_consum:0,handle_input:0,handle_r:0,handle_run_data:0,handle_state_chang:0,handler:0,happen:0,has:[0,5],hash:[0,3],have:[0,3,4],help:4,here:0,hierarch:0,hierarchi:0,hinder:5,howev:0,html:0,http:[0,4],ideal:0,idealist:0,ident:0,identifi:0,imag:[0,5],implement:0,implicitli:0,includ:0,increas:0,index:[0,2],inf:0,infer:0,infinit:0,info:0,inform:0,initi:0,initialize_context:0,initialize_input:0,initialize_output:0,inject:0,inject_context_attr:0,inner:0,input:[0,3,4],inspect:0,instal:2,instanc:3,instanti:4,instead:0,integr:0,intellig:0,intend:0,intern:[0,2],interv:0,introduct:4,invok:0,is_al:0,item:0,iter:0,itself:0,job:[0,1],jobsrun:1,join:0,just:0,keep:0,kei:0,key_fn:0,keyword:0,kind:0,kubeclust:0,kubernet:0,kwarg:0,label:0,lambda:0,last:0,latest:0,lazyasyncqueu:0,level:0,like:0,limit:5,link:0,list:0,load:0,local:[0,2],localcach:0,localclust:0,lock:[0,3],log:0,logger:0,logic:[0,5],look:0,loop:[0,1,5],machin:[0,1],made:0,mai:[0,3,4],main:[0,2,5],main_loop:0,mainli:1,maintain:0,make:0,manag:0,mani:0,manipul:4,map:[0,4,5],marker:0,master:0,match:0,max:0,max_level:0,mean:0,meant:0,mechan:0,meet:0,merg:[0,5],messag:0,met:0,method:[0,5],min:0,miscellan:0,mix:[0,5],model:2,modul:2,more:0,most:[0,4],move:0,multipl:[0,3],must:0,n_worker:0,name:[0,5],need:[0,5],need_skip:0,needd:0,network:0,new_stat:0,nextflow:4,non:0,none:0,normal:0,note:[0,4],noth:0,num:0,num_out:0,num_output:0,number:0,obj:0,object:[0,3,4,5],occur:0,offic:1,old_stat:0,on_complet:0,on_next:0,onc:[0,3],one:0,onli:[0,3],oper:2,opposit:0,option:[0,5],order:0,ordinari:0,org:0,organ:0,origin:5,other:[4,5],other_info:3,otherwis:[0,5],out:0,output:[0,4],output_ch:3,over:0,overrid:0,own:5,page:2,pair:0,parallel:[0,3,4],paramet:0,parent_flow_workdir:0,pars:0,part:4,pass:0,path:0,pathlib:0,pend:0,per:0,period:0,persist:0,person:0,pickl:0,pip:4,pipe:0,place:0,pnpnpn:0,point:0,pool:[0,3],posit:0,possibl:1,potenti:0,predefin:5,predic:0,prefect:0,prefecthq:0,present:0,prev_stat:0,print:0,problem:0,process:[0,2],produc:0,program:0,properti:0,provid:0,publish:0,push:0,put:[0,4,5],put_nowait:0,putter:0,pyflow:4,pypi:4,python:[0,5],queue:[0,4],queue_factori:0,queuechannel:0,quick:2,rai:2,rais:0,ran:[0,3],randomli:0,raw:0,read:0,real:0,receiv:3,record:[0,4],reduc:0,refer:2,regist:0,relat:0,remov:0,repres:[0,4],rerun:0,res:0,reservoir:0,resolv:0,resourc:0,respect:0,respond:0,result:0,retri:0,run:[0,3,4,5],run_job:[3,5],run_kei:[0,3],run_lock:0,run_task_timeout:0,run_timeout_sign:0,run_timeout_thread:0,run_workdir:0,rundatafilenotfounderror:0,rundatatypeerror:0,runexcept:0,runnabl:0,runner:2,runnerexecuteerror:0,runnerexecutor:0,runtask:0,runtimeerror:0,safe:0,same:[0,1,3,5],sampl:0,scale:0,schedul:[0,3,5],scope:0,script:5,search:2,second:[0,3],section:4,select:0,self:0,send:0,send_log:0,separ:0,sequenc:0,sequenti:0,serial:0,server_address:0,servic:1,set:[0,4],settl:0,setup:0,sever:4,share:3,shell:2,shellflow:0,shelltask:[0,5],shelltaskexecuteerror:0,should:[0,4,5],sigalarm:0,signal:0,signatur:0,similar:[0,4],simpl:0,simpli:0,simplic:0,simultan:0,sinc:[3,5],singl:[0,1],size:0,skip:[0,4],skip_fn:0,some:0,sourc:[0,4],specif:0,specifi:[0,5],split:0,src:0,standard:0,start:[0,2],start_execut:0,start_loop:0,state:2,state_typ:0,statu:0,stdin:0,stdout:0,still:0,stop:0,store:0,str:0,strict:0,string:0,subclass:[0,5],submit:0,subprocess:0,subscrib:0,subsequ:0,success:0,sum:[0,4],support:[0,4,5],syntax:0,system:0,tag:0,take:0,taken:0,target:2,task:[2,3],task_config:0,task_full_nam:0,task_hash:0,task_id:0,task_kei:[0,3],task_label:0,task_nam:0,task_runn:2,task_workdir:[0,3],taskrun:0,taskrun_id:0,taskrunn:0,taskschedul:0,tell:0,temporari:0,termin:0,test:0,than:0,thei:0,them:0,theoret:0,thereof:0,thi:[0,4,5],third:0,thread:[0,5],three:[0,5],through:0,thu:0,time:[0,3],timeout:0,timeout_decor:0,top:0,top_flow_workdir:0,torn:0,torun:1,total:5,trace_back:0,treat:0,trigger:0,tupl:0,turn:0,two:0,type:[0,5],typic:2,ugli:0,unhandl:0,union:0,uniqu:[0,3],unit:[0,4],unix:0,until:0,updat:0,update_run_st:0,usag:[4,5],use:0,used:[0,1,3,5],useful:0,user:[0,4,5],uses:0,using:[0,5],usual:0,util:0,valu:0,variabl:0,via:0,view:0,virtual:0,visit:0,wai:[0,5],wait:0,what:0,when:[0,5],whether:0,which:0,who:4,whole:[0,5],whose:0,within:0,work:[0,3],workdir:0,worker:0,workflow:2,would:0,wrap:0,wrape:5,write:0,xxxx:0,you:[0,4,5],your:[0,4,5],zhqu1148980644:4},titles:["API Reference","Executor","Welcome to flowsaber\u2019s documentation!","Internal","Quick Start","Task"],titleterms:{add:5,api:0,base:0,cach:0,channel:[0,4],cli:0,concept:4,context:0,custom:5,document:2,envtask:5,executor:[0,1],flow:[0,4],flow_runn:0,flowsab:2,indic:2,instal:4,intern:3,local:1,main:4,model:0,oper:[0,4,5],process:1,quick:4,rai:1,refer:0,runner:0,shell:0,start:4,state:0,tabl:2,target:0,task:[0,4,5],task_runn:0,typic:4,welcom:2,workflow:4}})