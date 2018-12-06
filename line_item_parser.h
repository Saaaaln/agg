
struct LineItem {
    int order_key;
    int part_key;
    int supp_key;
    int line_number;
    double quantity;
    double extended_price;
    double discount;
    double tax;
    char return_flag;
    char line_status;
    tm ship_date;
    tm commit_date;
    tm receipt_date;
    char ship_instruct[25];
    char ship_mode[10];
    char comment[44];

    friend std::ostream &operator<<(std::ostream &output, const LineItem &item) {
        output << item.order_key << "|" << item.quantity << "|";
        return output;
    }
//    friend std::ostream &operator<<(std::ostream &output, const LineItem &item) {
//        output << "LineItem: {Order key: " << item.order_key << "; Part key: " << item.part_key << "; Supp key: " << item.supp_key << "}";
//        return output;
//    }
};


std::vector<LineItem> &ReadAndParseLineItems(const std::string &file_path);

void ReadAndProjAllLineItems(const std::string &file_path);


struct ProjLineItem {
    int order_key;
    double quantity;
};

std::vector<ProjLineItem> &ReadAndParseProjLineItems(const std::string &file_path);
