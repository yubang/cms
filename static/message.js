var bottom = 20,opacity = 100;
setInterval(dealPic, 130);

//处理图片偏移宽度
var width=document.body.clientWidth/2-30;
document.getElementById("up-slide").style.left=width+"px";

var swiper = new Swiper('.swiper-container', {
    pagination: '.swiper-pagination',
    direction: 'vertical',
    slidesPerView: 1,
    paginationClickable: true,
    spaceBetween: 30,
    mousewheelControl: true
});

function dealPic() {

    obj = document.getElementById("up-slide");
    opacity = parseInt(opacity) - 15;
    if (opacity < 0) {
        opacity = 100;
    }
    obj.style.opacity = opacity / 100.00;

    bottom = bottom + 3;
    if (bottom > 40) bottom = 20;
    obj.style.bottom = bottom + "px";

    //console.log(obj.style.opacity);
}
