$(document).ready(function(){
    setInterval("showCode()",100);
    keepFocus();
});

function showCode(){
    $("#showCodeDiv").html($("#content").val());
}

function keepFocus(){
    var obj=document.getElementById("content");
    obj.onkeydown = function(event){
	event = event || window.event;
	if(event.keyCode == 9){
		insertText(obj,"    ");
		return false;
	}
}
}

function insertText(obj,str) {
    if (document.selection) {
        var sel = document.selection.createRange();
        sel.text = str;
    } else if (typeof obj.selectionStart === 'number' && typeof obj.selectionEnd === 'number') {
        var startPos = obj.selectionStart,
            endPos = obj.selectionEnd,
            cursorPos = startPos,
            tmpStr = obj.value;
        obj.value = tmpStr.substring(0, startPos) + str + tmpStr.substring(endPos, tmpStr.length);
        cursorPos += str.length;
        obj.selectionStart = obj.selectionEnd = cursorPos;
    } else {
        obj.value += str;
    }
}
