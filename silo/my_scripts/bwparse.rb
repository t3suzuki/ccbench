dimm = ""
h = Hash.new
ARGF.each { |line|
  if line =~ /mode/
    puts line
    dimm = ""
    h = Hash.new
  end
  if line =~ /(\w+)=(\w+)/
    k = $1
    v = $2
    if k =~ /DimmID/
      dimm = v
    elsif k =~ /Total\w+s$/
      if h[dimm+k]
        h[dimm+k] << v
      else
        h[dimm+k] = [v]
      end
    end
  end
  #if line =~ /USED/ or line =~ /prefe/
  if line =~ /tps/
    h.each { |k,v|
      n = v[1].to_i(16) - v[0].to_i(16)
      #puts "#{k} #{n}"
      if k =~ /0x0001TotalMedia/
        #puts "#{k} #{n} #{n * 256 / 10 / 1000.0 / 1000.0 / 1000.0} GB/s"
        puts "#{k} #{n} #{n * 64 / 10 / 1000.0 / 1000.0 / 1000.0} GB/s"
      end
    }
  end
}


