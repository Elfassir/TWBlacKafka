<html>
<head>
    <title>Color Test</title>
  <style>
    .colorBlock {
      width: 40px;
      height: 40px;
    }
    #Black {
      float: left;
      background-color: #000;
      color: white;
    }
    #White {
      float: left;
      background-color: white;
      color:black;
      margin-right: 40px;    	
    }
 
  </style>
</head>
<body>
  <div class="colorBlock" id="Black"></div>
  <div class="colorBlock" id="White"></div>
  
     <script src="http://code.jquery.com/jquery-1.11.0.min.js" type="text/javascript" charset="utf-8"></script>
    <script type="text/javascript" charset="utf-8">
   //Create anonymous function that listens for a click on anything with the class colorBlock
    $(".colorBlock").click(function() {
      //Get the background color of the clicked color block by using the this object
      var $backgroundColor = $(this).css("background-color");
      var $color = $(this).css("color");
      //Set the body's background-color to the received background-color
      $("body").css("background-color", $backgroundColor);
      $("body").css("color", $color);
      localStorage.setItem("bg",$backgroundColor);
      localStorage.setItem("textColor",$color);
    });
  </script>
  
  	<script type="text/javascript">
  		var bgColor = localStorage.getItem("bg");
  		var txtColor = localStorage.getItem("textColor");
  		if(bgColor !== null){
     		document.body.style.backgroundColor = bgColor;
     		document.body.style.color = txtColor;
     	}
  	</script>
</body>
</html>
<div class="header">
    <div class="container">
        <h3 class="app-name brand"><a href="/">TWBlacKafka</a></h3>
    </div>
    <hr/>
</div>