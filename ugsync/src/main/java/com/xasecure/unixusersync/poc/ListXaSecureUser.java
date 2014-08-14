package com.xasecure.unixusersync.poc;


public class ListXaSecureUser
{
    private final String uid;
    private final String uname;
    private final String gid;
    
    public static ListXaSecureUser parseUser(final String userLine)
        throws InvalidUserException
    {
        final String   line;
        final String[] parts;

        if(userLine == null)
        {
            throw new IllegalArgumentException("userLine cannot be null");
        }

        line = userLine.trim();

        if(line.startsWith("#") || line.isEmpty())
        {
             return null;
        }

        parts = line.split(":");

        if(parts.length < 3)
        {
            throw new InvalidUserException(userLine + "must be in the format of name:passwd:gid[:userlist]", line);
        }

        try
        {
            final ListXaSecureUser       xaUser;
            final String       uname;
            final String	   uid;
            final String       gid;
          
            uname  = parts[0];
            uid    = parts[2];
            gid    = parts[3];
            
            xaUser = new ListXaSecureUser(uname, uid, gid);

            return xaUser;
        }
        catch(final NumberFormatException ex)
        {
            throw new InvalidUserException(userLine + " uid must be a number", line);
        }
    }

    public ListXaSecureUser(final String nm, final String userid, final String grpid )
    {
        uname    = nm;
        uid      = userid;
        gid 	 = grpid;
        
    }

    public String getGid()
    {
        return (gid);
    }

    public String getName()
    {
        return (uname);
    }

    public String getUid()
    {
        return (uid);
    }

   
    @Override
    public String toString()
    {
        final StringBuilder sb;

        sb = new StringBuilder();
        sb.append(uname);
        sb.append(":");
        sb.append(uid);
        sb.append(":");
        sb.append(gid);
        
        sb.setLength(sb.length() - 1);

        return (sb.toString());
    }
}

