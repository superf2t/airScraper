class item:
    order_by_listing=['airbnb_id','link','title','description','bedroom','bathroom','map_coordinates','location_id','date_added']
    order_by_location=['name','parent_id']
    order_by_calender=['listing_id','price','price_date','is_booked']
    def __init__(self,klass):
        self.klass=klass
        for key in klass.get_attributes():
            setattr(self,key,None)
    def values(self):
        if self.klass.__class__.__name__=='listing':
            return [getattr(self,key) for key in self.order_by_listing]
        elif self.klass.__class__.__name__=='location':
            return [getattr(self,key) for key in self.order_by_location]
        elif self.klass.__class__.__name__=='calender':
            return [getattr(self,key) for key in self.order_by_calender]

