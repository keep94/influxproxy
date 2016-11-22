package main

func main() {
	http.Handle(
		"/query",
		apiutil.NewHandler(
			func(req url.Values) (interface{}, error) {
				return nil, nil
			},
			nil,
		))

}

// Input:
// /query?db=scotty&epoch=ms&q=SELECT+mean%28%22value...
//
// Output:
// {
//   "results":
//     [
//       {"series":[
//          {"name":"/proc/cpu/sys",
//           "columns":["time","mean"],
//           "values":[
//              [1477180800000,null],
//              [1477267200000,6242.151989583925],
//            ]}]}]}
