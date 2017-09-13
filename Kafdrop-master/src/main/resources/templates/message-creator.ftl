<#import "lib/template.ftl" as template>
<#import "/spring.ftl" as spring />
<@template.header "Topic: ${topic.name}: Add Message">
   <style type="text/css">
       h1 { margin-bottom: 16px; }
       #messageFormPanel { margin-top: 16px; }
       .toggle-msg { float: left;}
       .bs-form-elem { height: 30px;} 
   </style>

  <script src="/js/message-inspector.js"></script>
</@template.header>

<h1 class="col threecol">Add Message --> Topic: <a href="/topic/${topic.name}">${topic.name}</a></h1>

<div id="messageFormPanel" class="bs-panel">
<form method="GET" action="/topic/${topic.name}/addMessage" id="messageForm" class="bs-form panel-body">

    <div class="bs-form-group inline">
        <label for="message">Message</label>
        <@spring.bind path="messageForm.message"/>
        <@spring.formInput path="messageForm.message" attributes='class="bs-form-elem ${spring.status.error?string("error", "")}"'/>
        <span class="error"><@spring.showErrors "<br/>"/></span>

        <button class="bs-btn primary" type="submit"><i class="fa fa-key"></i> Submit Message</button>
  
    </div>
</form>
</div>

<@spring.bind path="messageForm.*"/>
<div id="message-display">
     <script type="text/javascript">
  		document.getElementById("message").value='';
 	 </script>
   	<#if !(spring.status.error) && !(messageForm.empty)>
          ${messagesResult}
      
    </#if>
</div>

<@template.footer/>
